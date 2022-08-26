package sdb

import (
	"errors"
	"time"

	"sdb/bitcask"
)

// Set 设置key的value
func (db *SDB) Set(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 构造record
	record := &bitcask.LogRecord{
		Key:   key,
		Value: value,
	}

	keyDir, err := db.writeLogRecord(record, String)
	if err != nil {
		return err
	}
	// 索引放ar树中
	err = db.updateIndexTree(record, keyDir, true, String)
	return err
}

// SetEX 带过期时间的设置key的value
func (db *SDB) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 构造record
	record := &bitcask.LogRecord{
		Key:       key,
		Value:     value,
		ExpiredAt: time.Now().Add(duration).Unix(), // 就多了个过期时间
	}

	keyDir, err := db.writeLogRecord(record, String)
	if err != nil {
		return err
	}
	// 索引放ar树中
	err = db.updateIndexTree(record, keyDir, true, String)
	return err
}

// SetNX 如果不存在设置一个key的value，如果存在返回nil
func (db *SDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// GET
	_, err := db.getVal(key, String)

	// 其他错误
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	// 如果key存在
	if err == nil {
		return nil
	}
	// SET
	// 构造record
	record := &bitcask.LogRecord{
		Key:   key,
		Value: value,
	}

	keyDir, err := db.writeLogRecord(record, String)
	if err != nil {
		return err
	}
	// 索引放ar树中
	err = db.updateIndexTree(record, keyDir, true, String)
	return err
}

// Get 获取key的value
func (db *SDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(key, String)
}

// MGet 批量获取key的value
func (db *SDB) MGet(keys [][]byte) ([][]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	if len(keys) == 0 {
		return nil, ErrWrongNumberOfArgs
	}
	values := make([][]byte, len(keys))
	for i, key := range keys {
		val, err := db.getVal(key, String)
		if err != nil && !errors.Is(ErrKeyNotFound, err) {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

// Delete 追加写的方式删除
func (db *SDB) Delete(key []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	record := &bitcask.LogRecord{
		Key:  key,
		Type: bitcask.TypeDelete,
	}
	keyDir, err := db.writeLogRecord(record, String)
	if err != nil {
		return err
	}

	// 索引树删除key
	err = db.deleteIndexTree(key, keyDir, String)
	return err
}
