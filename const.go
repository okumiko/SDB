package sdb

const (
	logFileTypeNum = 5

	lockFileName = "FLOCK"
)

type DataType byte

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)
