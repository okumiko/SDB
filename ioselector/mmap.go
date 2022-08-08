package ioselector

import (
	"io"
	"os"
	"sdb/mmap"
)

// MMapSelector MMAP方式实现IOSelector
type MMapSelector struct {
	file *os.File
	buf  []byte //没有加锁，因为写不同的offset不会race
	cap  int64
}

func NewMMapSelector(fileName string, fileSize int64) (IOSelector, error) {
	if fileSize <= 0 {
		return nil, ErrInvalidFileSize
	}
	file, err := openFile(fileName, fileSize)
	if err != nil {
		return nil, err
	}
	buf, err := mmap.Mmap(file, true, int(fileSize))
	if err != nil {
		return nil, err
	}

	return &MMapSelector{file: file, buf: buf, cap: int64(len(buf))}, nil
}

func (m *MMapSelector) Write(b []byte, offset int64) (int, error) {
	l := int64(len(b))
	if l <= 0 {
		return 0, nil
	}
	if offset < 0 || l+offset > m.cap {
		return 0, io.EOF
	}
	return copy(m.buf[offset:], b), nil
}

func (m *MMapSelector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset >= m.cap || offset+int64(len(b)) >= m.cap {
		return 0, io.EOF
	}
	return copy(b, m.buf[offset:]), nil
}

func (m *MMapSelector) Sync() error {
	return mmap.Msync(m.buf)
}

func (m *MMapSelector) Close() error {
	//先持久化
	if err := mmap.Msync(m.buf); err != nil {
		return err
	}
	//再取消映射
	if err := mmap.Munmap(m.buf); err != nil {
		return err
	}
	//最后关闭文件
	return m.file.Close()
}

func (m *MMapSelector) Delete() error {
	//取消映射
	if err := mmap.Munmap(m.buf); err != nil {
		return err
	}
	m.buf = nil //清空buf
	//清空文件
	if err := m.file.Truncate(0); err != nil {
		return err
	}
	//关闭文件
	if err := m.file.Close(); err != nil {
		return err
	}
	return os.Remove(m.file.Name())
}
