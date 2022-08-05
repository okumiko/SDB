package sdb

import (
	"sdb/art"
	"sdb/bitcask"
	"sdb/utils"
)

//hash结构：
//key->|field1->val1|filed2->val2|filed3->val3|...
//hash结构比较简单，一个key对应一棵ar树
//key和field编码生成hashKey
//文件中key｜field1->val1

//HSet ...
func (db *SDB) HSet(key, field, value []byte) error {
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	hashKey := utils.EncodeHashKey(key, field)
	//把hash key作为key写record，因为每条record需要知道他的key和field，建索引时需要
	//生成record
	record := &bitcask.LogRecord{Key: hashKey, Value: value}
	//record写入磁盘，返回keyDir
	keyDir, err := db.writeLogRecord(record, Hash)
	if err != nil {
		return err
	}

	if db.hashIndex.trees[string(key)] == nil {
		db.hashIndex.trees[string(key)] = art.NewART()
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(key)]

	//具体每颗索引树key是field
	//field->keyDir
	err = db.updateIndexTree(&bitcask.LogRecord{Key: field, Value: value},
		keyDir, true, Hash)
	return err
}

// HGet ...
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
