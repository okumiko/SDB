package options

import "time"

// StoreMode 存储模式
type StoreMode int

const (
	// MemoryMode 纯内存
	MemoryMode StoreMode = iota

	// BitCaskMode BitCask型
	BitCaskMode
)

// IOType IO类型
type IOType int8

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// MMap Memory Map.
	MMap
)

// Options for opening a db.
type Options struct {
	// 数据文件路径
	DBPath string

	// 节点存储类型
	StoreMode StoreMode

	// IO方式
	IoType IOType

	// 写操作是否立刻刷盘
	Sync bool

	// merge操作间隔时间
	LogFileMergeInterval time.Duration

	// 存储空间达到阈值的文件将会加入merge列表，从占用率从大到小进行merge
	LogFileMergeRatio float64

	// 每个文件的最大大小
	LogFileSizeThreshold int64

	// 向countFile发送的channel缓冲大小
	CountBufferSize int
}

func NewDefaultOptions(path string) Options {
	return Options{
		DBPath:               path,
		StoreMode:            BitCaskMode,
		IoType:               FileIO,
		Sync:                 false,
		LogFileMergeInterval: time.Hour * 8,
		LogFileMergeRatio:    0.5,
		LogFileSizeThreshold: 512 << 20,
		CountBufferSize:      8 << 20,
	}
}
