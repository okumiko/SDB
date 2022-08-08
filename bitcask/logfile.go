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
	// ErrInvalidCrc CRC校验失败
	ErrInvalidCrc = errors.New("invalid crc")

	// ErrWriteSizeNotEqual 写入的数据大小和buffer大小不相等
	ErrWriteSizeNotEqual = errors.New("write size is not equal to record size")

	// ErrEndOfRecord record结尾
	ErrEndOfRecord = errors.New("end of record in log file")

	// ErrUnsupportedIOType 只支持标准IO和MMAP
	ErrUnsupportedIOType = errors.New("unsupported io type")

	// ErrUnsupportedLogFileType 不支持的数据格式
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	// InitialLogFileId 文件id从0开始，全局变量
	InitialLogFileId = 0

	// FilePrefix 磁盘中的文件统一前缀
	FilePrefix = "log."
)

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
	// FileIO 标准IO.
	FileIO IOType = iota
	// MMap 内存映射IO
	MMap
)

//LogFile 读写磁盘文件的抽象
type LogFile struct {
	sync.RWMutex
	FileID      uint32                //文件id
	WriteOffSet int64                 //追加写的offset
	IoSelector  ioselector.IOSelector //IO接口
}

//OpenLogFile 根据指定路径打开文件或者新建文件
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

//getLogFileName 拼接文件全路径
func (lf *LogFile) getLogFileName(path string, fid uint32, fType FileType) (name string, err error) {
	if _, ok := FileNameMap[fType]; !ok {
		return "", ErrUnsupportedLogFileType
	}

	fName := FileNameMap[fType] + fmt.Sprintf("%010d", fid)
	name = filepath.Join(path, fName) //example: path/log.string.010
	return
}

//readBytes 读取文件指定大小字节
func (lf *LogFile) readBytes(offset, n int64) (buf []byte, err error) {
	buf = make([]byte, n)
	_, err = lf.IoSelector.Read(buf, offset)
	return
}

//ReadLogRecord 根据 offset 从文件读出logRecord
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

	// 读出key&value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := lf.readBytes(offset+headerSize, keySize+valueSize)
		if err != nil {
			return nil, 0, err
		}
		lr.Key = kvBuf[:keySize]
		lr.Value = kvBuf[keySize:]
	}

	// crc校验
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

// Sync 刷盘
func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

// Close 关闭读写
func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

// Delete 删除文件
// File can`t be retrieved if do this, so use it carefully.
func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}
