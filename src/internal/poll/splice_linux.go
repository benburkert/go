package poll

import (
	"sync/atomic"
	"syscall"
	"unsafe"
)

const errEOF = syscall.Errno(0x0)

// Splice transfers at most remain bytes of data from src to dst, using the
// splice system call to minimize copies of data from and to userspace.
//
// Splice creates a pipe to serve as a buffer for the data transfer. Both src
// and dst must both be stream-oriented sockets.
//
// If err != nil, sc is the system call which caused the error.
func Splice(dst, src *FD, remain int64) (written int64, handled bool, sc string, err error) {
	pipe, sc, err := newPipe()
	if err != nil {
		return 0, false, sc, err
	}
	defer pipe.release()

	// From here on, the operation should be considered handled,
	// even if Splice doesn't transfer any data.
	if err := src.readLock(); err != nil {
		return 0, true, "splice", err
	}
	defer src.readUnlock()
	if err := dst.writeLock(); err != nil {
		return 0, true, "splice", err
	}
	defer dst.writeUnlock()
	if err := src.pd.prepareRead(src.isFile); err != nil {
		return 0, true, "splice", err
	}
	if err := dst.pd.prepareWrite(dst.isFile); err != nil {
		return 0, true, "splice", err
	}

	var dstEAGAIN, srcEAGAIN, seenEOF bool
	for {
		switch {
		case seenEOF && pipe.data == 0:
			// saw src EOF and pipe is empty, splice is finished

			return written, true, "", nil
		case !dstEAGAIN && pipe.data > 0:
			// dst might be ready and pipe has data, try pumping data to dst

			n, err := pipe.pumpTo(dst)
			if err == syscall.EAGAIN {
				dstEAGAIN = true
				continue
			}
			if err != nil {
				return written, true, "splice", err
			}

			written += int64(n)
			remain -= int64(n)
		case pipe.data == 0 && srcEAGAIN && !seenEOF:
			// no pipe data and src would block, wait for src to be ready

			if err := src.pd.waitRead(src.isFile); err != nil {
				return written, true, "splice", err
			}
			srcEAGAIN = false
		case pipe.data == 0 && !srcEAGAIN:
			// no data and src might be ready, try draining from src

			fallthrough
		case !srcEAGAIN && !seenEOF && pipe.data < pipe.size:
			// pipe has data but dst would block, also the pipe is not full and src might be ready, try draining from src

			err := pipe.drainFrom(src, int(remain))
			if err == syscall.EAGAIN {
				srcEAGAIN = true
				continue
			}
			if err == errEOF {
				seenEOF = true
				continue
			}
			if err != nil {
				return written, true, "splice", err
			}
		case pipe.data >= pipe.size:
			// pipe is full and dst would block, wait for dst to be ready

			if err := dst.pd.waitWrite(dst.isFile); err != nil {
				return written, true, "splice", err
			}
			dstEAGAIN = false
		default:
			// the pipe has data but is not full and both dst & src would
			// block, and waiting on both src & dst is not possible, so wait on
			// dst to be ready, and then assume src might be ready too

			if err := dst.pd.waitWrite(dst.isFile); err != nil {
				return written, true, "splice", err
			}
			dstEAGAIN, srcEAGAIN = false, false
		}
	}
}

type pipe struct {
	fds  [2]int
	data int
	size int
}

var disableSplice unsafe.Pointer

func newPipe() (*pipe, string, error) {
	b := (*bool)(atomic.LoadPointer(&disableSplice))
	if b != nil && *b {
		return nil, "splice", syscall.EINVAL
	} else if b == nil {
		b = new(bool)
		defer atomic.StorePointer(&disableSplice, unsafe.Pointer(b))
	}

	p := new(pipe)
	if sc, err := p.alloc(); err != nil {
		*b = false
		return nil, sc, err
	}
	return p, "", nil
}

// drainFrom moves data from a sock to p.
func (p *pipe) drainFrom(sock *FD, max int) error {
	if free := p.size - p.data; max > free {
		max = free
	}

	n, err := splice(p.fds[1], sock.Sysfd, max)
	if err != nil {
		return err
	}
	if n == 0 {
		return errEOF
	}

	p.data += n
	return nil
}

// pumpTo moves buffered data from p to sock.
func (p *pipe) pumpTo(sock *FD) (int, error) {
	n, err := splice(sock.Sysfd, p.fds[0], p.data)
	if err != nil {
		return n, err
	}

	p.data -= n
	return n, nil
}

const (
	// spliceNonblock makes calls to splice(2) non-blocking.
	spliceNonblock = 0x2
)

// splice wraps the splice system call. Since the current implementation
// only uses splice on sockets and pipes, the offset arguments are unused.
// splice returns int instead of int64, because callers never ask it to
// move more data in a single call than can fit in an int32.
func splice(out, in, max int) (int, error) {
	n, err := syscall.Splice(in, nil, out, nil, max, spliceNonblock)
	return int(n), err
}

func (p *pipe) alloc() (string, error) {
	// pipe2 was added in 2.6.27 and our minimum requirement is 2.6.23, so it
	// might not be implemented. Falling back to pipe is possible, but prior to
	// 2.6.27 splice returns EAGAIN instead of EOF when the connect is closed.
	const flags = syscall.O_CLOEXEC | syscall.O_NONBLOCK
	if err := syscall.Pipe2(p.fds[:], flags); err != nil {
		return "pipe2", err
	}

	// F_GETPIPE_SZ was added in 2.6.35, which does not have the -EAGAIN bug.
	size, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(p.fds[0]), syscall.F_GETPIPE_SZ, 0)
	if errno != 0 {
		p.release()
		return "fcntl", errno
	}
	p.size = int(size)

	return "", nil
}

func (p *pipe) release() error {
	err := CloseFunc(p.fds[0])
	err1 := CloseFunc(p.fds[1])
	if err == nil {
		return err1
	}
	return err
}
