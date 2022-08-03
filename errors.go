package sdb

import "errors"

var (
	// ErrKeyNotFound key not found
	ErrKeyNotFound = errors.New("key not found")

	// ErrLogFileNotFound log file not found
	ErrLogFileNotFound = errors.New("log file not found")

	// ErrWrongNumberOfArgs doesn't match key-value pair numbers
	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")

	//ErrMergeRunning lock the SDB when log file is merging
	ErrMergeRunning = errors.New("log file merge is running, retry later")
)
