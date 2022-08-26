package mmap

import (
	"os"
)

// MMap mmap系统调用,返回内存，建立和物理地址的映射
func MMap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// MUnmap munmap系统调用，释放空间，接触映射
func MUnmap(b []byte) error {
	return munmap(b)
}

// MSync msync系统调用，把内核缓冲区（虚拟内存指向的地方）刷盘
func MSync(b []byte) error {
	return msync(b)
}
