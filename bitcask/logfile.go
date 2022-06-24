package bitcask

import (
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"sdb/ioselector"
	"sync"
	"sync/atomic"
)

var (
	// ErrInvalidCrc invalid crc.
	ErrInvalidCrc = errors.New("logfile: invalid crc")

	// ErrWriteSizeNotEqual write size is not equal to entry size.
	ErrWriteSizeNotEqual = errors.New("logfile: write size is not equal to entry size")

	// ErrEndOfEntry end of entry in log file.
	ErrEndOfRecord = errors.New("logfile: end of entry in log file")

	// ErrUnsupportedIoType unsupported io type, only mmap and fileIO now.
	ErrUnsupportedIOType = errors.New("unsupported io type")

	// ErrUnsupportedLogFileType unsupported log file type, only WAL and ValueLog now.
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	// InitialLogFileId initial log file id: 0.
	InitialLogFileId = 0

	// FilePrefix log file prefix.
	FilePrefix = "log."
)

// 文件类型
type FileType byte

const (
	Str FileType = iota
	List
	Hash
	Set
	ZSet
)

var (
	FileNameMap = map[FileType]string{
		Str:  "log.string.",
		List: "log.list.",
		Hash: "log.hash.",
		Set:  "log.set.",
		ZSet: "log.zset.",
	}

	FileTypesMap = map[string]FileType{
		"str":  Str,
		"list": List,
		"hash": Hash,
		"set":  Set,
		"zset": ZSet,
	}
)

type IOType byte

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// MMap Memory Map.
	MMap
)

//读写磁盘文件的抽象
type LogFile struct {
	sync.RWMutex
	FileID      uint32
	WriteOffSet int64
	IoSelector  ioselector.IOSelector
}

// 打开log file或者创建log file
func OpenLogFile(path string, fID uint32, fSize int64, fType FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{FileID: fID}
	fileName, err := lf.getLogFileName(path, fID, fType)
	if err != nil {
		return nil, err
	}

	var selector ioselector.IOSelector
	switch ioType {
	case FileIO:
		if selector, err = ioselector.NewStandardIOSelector(fileName, fSize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fileName, fSize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIOType
	}

	lf.IoSelector = selector
	return
}

func (lf *LogFile) getLogFileName(path string, fid uint32, fType FileType) (name string, err error) {
	if _, ok := FileNameMap[fType]; !ok {
		return "", ErrUnsupportedLogFileType
	}

	fName := FileNameMap[fType] + fmt.Sprintf("%010d", fid)
	name = filepath.Join(path, fName) //example:path/log.string.010
	return
}

//读为切片
func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}

//根据 offset 从 logfile 读 logRecord
func (lf *LogFile) ReadLogRecord(offset int64) (lr *LogRecord, recordSize int64, err error) {
	//read recordHead
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := decodeHeader(headerBuf)

	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfRecord
	}

	lr = &LogRecord{
		ExpiredAt: header.expiredAt,
		Type:      header.typ,
	}
	keySize, valueSize := int64(header.kSize), int64(header.vSize)
	recordSize = headerSize + keySize + valueSize

	//read kv
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := lf.readBytes(offset+headerSize, keySize+valueSize)
		if err != nil {
			return nil, 0, err
		}
		lr.Key = kvBuf[:keySize]
		lr.Value = kvBuf[keySize:]
	}

	//crc check
	if crc := getRecordCrc(lr, headerBuf[crc32.Size:headerSize]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return
}

// 追加写logfile
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}
	offset := atomic.LoadInt64(&lf.WriteOffSet)
	n, err := lf.IoSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}

	//offset后移
	atomic.AddInt64(&lf.WriteOffSet, int64(n))
	return nil
}

// Sync commits the current contents of the log file to stable storage.
func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

// Close current log file.
func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

// Delete delete current log file.
// File can`t be retrieved if do this, so use it carefully.
func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}