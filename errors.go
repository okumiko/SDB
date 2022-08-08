package sdb

import "errors"

var (
	// ErrKeyNotFound key不存在
	ErrKeyNotFound = errors.New("key not found")

	// ErrLogFileNotFound 磁盘存储文件未找到
	ErrLogFileNotFound = errors.New("log file not found")

	// ErrWrongNumberOfArgs 参数个数不匹配
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")

	//ErrMergeRunning 文件进行merge时无法再进行merge
	ErrMergeRunning = errors.New("log file merge is running, retry later")
)
