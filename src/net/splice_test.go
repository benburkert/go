// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build linux

package net

import (
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

func TestSplice(t *testing.T) {
	t.Run("tcp-to-tcp", func(t *testing.T) { testSplice(t, "tcp", "tcp") })
	t.Run("unix-to-tcp", func(t *testing.T) { testSplice(t, "unix", "tcp") })
}

func testSplice(t *testing.T, upNet, downNet string) {
	t.Run("simple", spliceTestCase{upNet, downNet, 128, 128, 0}.test)
	t.Run("multipleWrite", spliceTestCase{upNet, downNet, 4096, 1 << 20, 0}.test)
	t.Run("big", spliceTestCase{upNet, downNet, 1<<31 - 1, 1<<31 - 1, 0}.test)
	t.Run("honorsLimitedReader", spliceTestCase{upNet, downNet, 4096, 1 << 20, 1 << 10}.test)
}

type spliceTestCase struct {
	upNet, downNet string

	chunkSize, totalSize int
	limitReadSize        int
}

func (tc spliceTestCase) test(t *testing.T) {
	clientUp, serverUp, err := spliceTestSocketPair(tc.upNet)
	if err != nil {
		t.Fatal(err)
	}
	defer serverUp.Close()

	cleanup, err := startSpliceClient(clientUp, "w", tc.chunkSize, tc.totalSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	clientDown, serverDown, err := spliceTestSocketPair(tc.downNet)
	if err != nil {
		t.Fatal(err)
	}
	defer serverDown.Close()

	cleanup, err = startSpliceClient(clientDown, "r", tc.chunkSize, tc.totalSize)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	var (
		r    io.Reader = serverUp
		size           = tc.totalSize
	)

	if tc.limitReadSize > 0 {
		size = tc.limitReadSize
		r = &io.LimitedReader{
			N: int64(tc.limitReadSize),
			R: serverUp,
		}

		defer serverUp.Close()
	}

	n, err := io.Copy(serverDown, r)
	serverDown.Close()
	if err != nil {
		t.Fatal(err)
	}

	if want := int64(size); want != n {
		t.Errorf("want %d bytes spliced, got %d", want, n)
	}
}

func BenchmarkSplice(b *testing.B) {
	testHookUninstaller.Do(uninstallTestHooks)

	b.Run("tcp-to-tcp", func(b *testing.B) { benchSplice(b, "tcp", "tcp") })
	b.Run("unix-to-tcp", func(b *testing.B) { benchSplice(b, "unix", "tcp") })
}

func benchSplice(b *testing.B, upNet, downNet string) {
	for i := 0; i <= 10; i++ {
		chunkSize := 1 << uint(i+10)
		tc := spliceTestCase{
			upNet:     upNet,
			downNet:   downNet,
			chunkSize: chunkSize,
		}

		b.Run(strconv.Itoa(chunkSize), tc.bench)
	}
}

func (tc spliceTestCase) bench(b *testing.B) {
	// To benchmark the genericReadFrom code path, set this to false.
	useSplice := true

	clientUp, serverUp, err := spliceTestSocketPair(tc.upNet)
	if err != nil {
		b.Fatal(err)
	}
	defer serverUp.Close()

	cleanup, err := startSpliceClient(clientUp, "w", tc.chunkSize, tc.chunkSize*b.N)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	clientDown, serverDown, err := spliceTestSocketPair(tc.downNet)
	if err != nil {
		b.Fatal(err)
	}
	defer serverDown.Close()

	cleanup, err = startSpliceClient(clientDown, "r", tc.chunkSize, tc.chunkSize*b.N)
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	b.SetBytes(int64(tc.chunkSize))
	b.ResetTimer()

	if useSplice {
		_, err := io.Copy(serverDown, serverUp)
		if err != nil {
			b.Fatal(err)
		}
	} else {
		type onlyReader struct {
			io.Reader
		}
		_, err := io.Copy(serverDown, onlyReader{serverUp})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func spliceTestSocketPair(net string) (client, server Conn, err error) {
	ln, err := newLocalListener(net)
	if err != nil {
		return nil, nil, err
	}
	defer ln.Close()
	var cerr, serr error
	acceptDone := make(chan struct{})
	go func() {
		server, serr = ln.Accept()
		acceptDone <- struct{}{}
	}()
	client, cerr = Dial(ln.Addr().Network(), ln.Addr().String())
	<-acceptDone
	if cerr != nil {
		if server != nil {
			server.Close()
		}
		return nil, nil, cerr
	}
	if serr != nil {
		if client != nil {
			client.Close()
		}
		return nil, nil, serr
	}
	return client, server, nil
}

func startSpliceClient(conn Conn, op string, chunkSize, totalSize int) (func(), error) {
	f, err := conn.(interface{ File() (*os.File, error) }).File()
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = []string{
		"GO_NET_TEST_SPLICE=1",
		"GO_NET_TEST_SPLICE_OP=" + op,
		"GO_NET_TEST_SPLICE_CHUNK_SIZE=" + strconv.Itoa(chunkSize),
		"GO_NET_TEST_SPLICE_TOTAL_SIZE=" + strconv.Itoa(totalSize),
	}
	cmd.ExtraFiles = append(cmd.ExtraFiles, f)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	donec := make(chan struct{})
	go func() {
		cmd.Wait()
		conn.Close()
		f.Close()
		close(donec)
	}()

	return func() { <-donec }, nil
}

func init() {
	if os.Getenv("GO_NET_TEST_SPLICE") == "" {
		return
	}
	defer os.Exit(0)

	f := os.NewFile(uintptr(3), "splice-test-conn")
	defer f.Close()

	conn, err := FileConn(f)
	if err != nil {
		log.Fatal(err)
	}

	var chunkSize int
	if chunkSize, err = strconv.Atoi(os.Getenv("GO_NET_TEST_SPLICE_CHUNK_SIZE")); err != nil {
		log.Fatal(err)
	}
	buf := make([]byte, chunkSize)

	var totalSize int
	if totalSize, err = strconv.Atoi(os.Getenv("GO_NET_TEST_SPLICE_TOTAL_SIZE")); err != nil {
		log.Fatal(err)
	}

	var fn func([]byte) (int, error)
	switch op := os.Getenv("GO_NET_TEST_SPLICE_OP"); op {
	case "r":
		fn = conn.Read
	case "w":
		defer conn.Close()

		fn = conn.Write
	default:
		log.Fatalf("unknown op %q", op)
	}

	var n int
	for count := 0; count < totalSize; count += n {
		var err error
		if n, err = fn(buf); err != nil {
			return
		}
	}
}
