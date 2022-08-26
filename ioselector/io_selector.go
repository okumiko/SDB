package ioselector

import (
	"errors"
	"os"
)

// DefaultFilePerm 默认普通文件，文件所有者对该文件有读写权限，用户组和其他人只有读权限，
const DefaultFilePerm = 0644

var ErrInvalidFileSize = errors.New("file size can`t be zero or negative")

// IOSelector 文件抽象接口
type IOSelector interface {
	Write(b []byte, offset int64) (int, error)

	Read(b []byte, offset int64) (int, error)

	Sync() error

	Close() error

	Delete() error
}

func openFile(fileName string, fileSize int64) (*os.File, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, DefaultFilePerm)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// 打开文件空间不足
	if stat.Size() < fileSize {
		// 重新分配空间
		if err = file.Truncate(fileSize); err != nil {
			return nil, err
		}
	}
	return file, nil
}
