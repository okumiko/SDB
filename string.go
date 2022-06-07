package sdb

import (
	"errors"
	"sdb/bitcask"
	"time"
)

// Set key to hold the string value. If key already holds a value,overwrite it.
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

// SetEX set key to hold the string value and set key to timeout after the given duration.
func (db *SDB) SetEX(key, value []byte, duration time.Duration) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	// 构造record
	record := &bitcask.LogRecord{
		Key:       key,
		Value:     value,
		ExpiredAt: time.Now().Add(duration).Unix(), //就多了个过期时间
	}

	keyDir, err := db.writeLogRecord(record, String)
	if err != nil {
		return err
	}
	// 索引放ar树中
	err = db.updateIndexTree(record, keyDir, true, String)
	return err
}

// SetNX sets the key-value pair if it is not exist. It returns nil if the key already exists.
func (db *SDB) SetNX(key, value []byte) error {
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	//GET
	_, err := db.getVal(key, String)

	//其他错误
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	//如果key存在
	if err == nil {
		return nil
	}
	//SET
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

// Get get the value of key.
// If the key does not exist the error ErrKeyNotFound is returned.
func (db *SDB) Get(key []byte) ([]byte, error) {
	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	return db.getVal(key, String)
}

// MGet get the values of all specified keys.
// If the key that does not hold a string value or does not exist, nil is returned.
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

// 追加写的方式删除
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

	//索引树删除key
	err = db.deleteIndexTree(record, keyDir, String)
	return err
}
