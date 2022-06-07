package ioselector

import (
	"os"
)

type StandardIOSelector struct {
	file *os.File
}

func NewStandardIOSelector(fileName string, fileSize int64) (IOSelector, error) {
	if fileSize <= 0 {
		return nil, ErrInvalidFileSize
	}
	file, err := openFile(fileName, fileSize)
	if err != nil {
		return nil, err
	}
	return &StandardIOSelector{file: file}, nil
}

func (sio *StandardIOSelector) Write(b []byte, offset int64) (int, error) {
	return sio.file.WriteAt(b, offset)
}

func (sio *StandardIOSelector) Read(b []byte, offset int64) (int, error) {
	return sio.file.ReadAt(b, offset)
}

func (sio *StandardIOSelector) Sync() error {
	return sio.file.Sync()
}

func (sio *StandardIOSelector) Close() error {
	//先持久化
	if err := sio.Sync(); err != nil {
		return err
	}
	return sio.file.Close()
}

func (sio *StandardIOSelector) Delete() error {
	//清空文件
	if err := sio.file.Truncate(0); err != nil {
		return err
	}
	if err := sio.file.Close(); err != nil {
		return err
	}
	return os.Remove(sio.file.Name())
}
