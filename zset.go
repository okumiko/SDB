package sdb

import (
	"sdb/art"
	"sdb/bitcask"
	"sdb/utils"
)

// ZAdd 设置指定key的有序集合的member的score
func (db *SDB) ZAdd(key []byte, score float64, value []byte) error {
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if err := db.zsetIndex.murmurhash.Write(value); err != nil {
		return err
	}
	sum := db.zsetIndex.murmurhash.EncodeSum128()
	db.zsetIndex.murmurhash.Reset()
	if db.zsetIndex.trees[string(key)] == nil {
		db.zsetIndex.trees[string(key)] = art.NewART()
	}
	db.zsetIndex.idxTree = db.zsetIndex.trees[string(key)]

	scoreBuf := []byte(utils.Float64ToStr(score))
	zsetKey := utils.EncodeZSetKey(key, scoreBuf)

	// key+score作为key
	record := &bitcask.LogRecord{Key: zsetKey, Value: value}
	keyDir, err := db.writeLogRecord(record, ZSet)
	if err != nil {
		return err
	}

	return db.updateIndexTree(&bitcask.LogRecord{Key: sum, Value: value}, keyDir, true, ZSet)
}
