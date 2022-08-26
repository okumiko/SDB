package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

// Mmap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// Munmap unmaps a previously mapped slice.
func munmap(b []byte) error {
	return unix.Munmap(b)
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
