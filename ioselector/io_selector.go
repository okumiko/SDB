package ioselector

import (
	"errors"
	"os"
)

//默认普通文件，文件所有者对该文件有读写权限，用户组和其他人只有读权限，
const DefaultFilePerm = 0644

var ErrInvalidFileSize = errors.New("fSize can`t be zero or negative")

//文件交互接口
type IOSelector interface {
	// Write a slice to log file at offset.
	// It returns the number of bytes written and an error, if any.
	Write(b []byte, offset int64) (int, error)

	// Read a slice from offset.
	// It returns the number of bytes read and any error encountered.
	Read(b []byte, offset int64) (int, error)

	// Sync commits the current contents of the file to stable storage.
	// Typically, this means flushing the file system's in-memory copy
	// of recently written data to disk.
	Sync() error

	// Close the File, rendering it unusable for I/O.
	// It will return an error if it has already been closed.
	Close() error

	// Delete the file.
	// Must close it before delete, and will unmap if in MMapSelector.
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

	//打开文件空间不足
	if stat.Size() < fileSize {
		//重新分配空间
		if err = file.Truncate(fileSize); err != nil {
			return nil, err
		}
	}
	return file, nil
}
