package sdb

import (
	"sdb/art"
	"sdb/bitcask"
	"sdb/utils"
)

func (db *SDB) HSet(key, field, value []byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	hashKey := utils.EncodeHashKey(key, field)
	//把hash key作为key写record，因为每条record需要知道他的key和field，建索引时需要
	record := &bitcask.LogRecord{Key: hashKey, Value: value}
	keyDir, err := db.writeLogRecord(record, Hash)
	if err != nil {
		return err
	}

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]

	//具体每颗索引树key是field
	err = db.updateIndexTree(&bitcask.LogRecord{Key: field, Value: value},
		keyDir, true, Hash)
	return err
}

// HGet returns the value associated with field in the hash stored at key.
func (db *SDB) HGet(key, field []byte) ([]byte, error) {
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	//一个key对应一个ar树
	if db.hashIndex.trees[string(key)] == nil {
		return nil, nil
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]
	val, err := db.getVal(field, Hash)
	if err == ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}
