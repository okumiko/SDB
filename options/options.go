package options

import "time"

// DataIndexMode 存储模式
type DataIndexMode int

const (
	// KeyValueMemMode 纯内存
	KeyValueMemMode DataIndexMode = iota

	// KeyOnlyMemMode BitCask型
	KeyOnlyMemMode
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
	IndexMode DataIndexMode

	// IO方式
	IoType IOType

	//写操作是否立刻刷盘
	Sync bool

	//merge操作间隔时间
	LogFileMergeInterval time.Duration

	// 存储空间达到阈值的文件将会加入merge列表，从占用率从大到小进行merge
	LogFileMergeRatio float64

	//每个文件的最大大小
	LogFileSizeThreshold int64

	//向countFile发送的channel缓冲大小
	CountBufferSize int
}

func DefaultOptions(path string) Options {
	return Options{
		DBPath:               path,
		IndexMode:            KeyOnlyMemMode,
		IoType:               FileIO,
		Sync:                 false,
		LogFileMergeInterval: time.Hour * 8,
		LogFileMergeRatio:    0.5,
		LogFileSizeThreshold: 512 << 20,
		CountBufferSize:      8 << 20,
	}
}
