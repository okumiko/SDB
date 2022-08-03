// Package flock
//pid锁，仅linux或mac或unix
package flock

import (
	"fmt"
	"os"
	"syscall"
)

type FileLock struct {
	name string
	fd   *os.File
}

//AcquireFileLock 文件锁,获取一个文件锁，如果加锁则返回错误
func AcquireFileLock(path string, readOnly bool) (*FileLock, error) {
	flag := os.O_RDWR // open the file read-write.
	if readOnly {
		flag = os.O_RDONLY // open the file read-only.
	}
	file, err := os.OpenFile(path, flag, 0)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(path, flag|os.O_CREATE, 0644)
	}
	if err != nil {
		return nil, err
	}
	//排他锁，写锁
	lockType := syscall.LOCK_EX | syscall.LOCK_NB
	if readOnly { //共享锁，读锁
		lockType = syscall.LOCK_SH | syscall.LOCK_NB
	}
	//在调用 flock 的时候，采用 LOCK_NB 参数。在尝试锁住该文件的时候，发现已经被其他服务锁住，会返回错误，错误码为 EWOULDBLOCK。
	if err = syscall.Flock(int(file.Fd()), lockType); err != nil {
		return nil, err
	}
	return &FileLock{fd: file, name: path}, nil
}

//SyncFileLock pageCache刷到磁盘
func SyncFileLock(path string) error {
	fd, err := os.Open(path)
	if err != nil {
		return err
	}
	err = fd.Sync()
	closeErr := fd.Close()
	if err != nil {
		return fmt.Errorf("sync dir err: %+v", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close dir err: %+v", err)
	}
	return nil
}

// Release 解锁
func (fl *FileLock) Release() error {
	if err := syscall.Flock(int(fl.fd.Fd()), syscall.LOCK_UN|syscall.LOCK_NB); err != nil {
		return err
	}
	return fl.fd.Close()
}
