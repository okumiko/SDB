package sdb

import (
	"sdb/art"
	"sdb/bitcask"
	"sdb/logger"
)

// SAdd 将指定的成员添加到存储在 key 的集合中。
// 已经是该集合成员的指定成员将被忽略。
// 如果key不存在，则在添加指定成员之前创建一个新集合。
func (db *SDB) SAdd(key []byte, members ...[]byte) error {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.setIndex.trees[string(key)] == nil {
		db.setIndex.trees[string(key)] = art.NewART()
	}
	db.setIndex.idxTree = db.setIndex.trees[string(key)]

	for _, mem := range members {
		if len(mem) == 0 {
			continue
		}
		//对mem算一个hash值，内存放hash值，value放磁盘
		if err := db.setIndex.murhash.Write(mem); err != nil {
			return err
		}
		sum := db.setIndex.murhash.EncodeSum128()
		db.setIndex.murhash.Reset()

		record := &bitcask.LogRecord{
			Key:   key,
			Value: mem,
		}
		valuePos, err := db.writeLogRecord(record, Set)
		if err != nil {
			return err
		}
		if err := db.updateIndexTree(&bitcask.LogRecord{Key: sum, Value: mem},
			valuePos, true, Set); err != nil {
			return err
		}
	}
	return nil
}

//SPop 从 key 处的设置值存储中删除并返回一个或多个随机成员。
func (db *SDB) SPop(key []byte, count uint) ([][]byte, error) {
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()
	if db.setIndex.trees[string(key)] == nil {
		return nil, nil
	}
	db.setIndex.idxTree = db.setIndex.trees[string(key)]

	var values [][]byte

	//遍历索引树
	it := db.setIndex.idxTree.Iterator()
	for it.HasNext() && count > 0 {
		count--
		node, _ := it.Next()
		if node == nil {
			continue
		}
		val, err := db.getVal(node.Key(), Set)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
	}
	for _, val := range values {
		if err := db.sremInternal(key, val); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (db *SDB) sremInternal(key []byte, member []byte) error {
	db.setIndex.idxTree = db.setIndex.trees[string(key)]

	if err := db.setIndex.murhash.Write(member); err != nil {
		return err
	}
	sum := db.setIndex.murhash.EncodeSum128()
	db.setIndex.murhash.Reset()

	val, updated := db.setIndex.idxTree.Delete(sum)
	if !updated {
		return nil
	}
	entry := &bitcask.LogRecord{Key: key, Value: sum, Type: bitcask.TypeDelete}
	pos, err := db.writeLogRecord(entry, Set)
	if err != nil {
		return err
	}

	db.sendCountChan(val, updated, Set)
	// The deleted entry itself is also invalid.
	_, size := bitcask.EncodeRecord(entry)
	node := &keyDir{fileID: pos.fileID, recordSize: size}
	select {
	case db.discards[Set].valChan <- node:
	default:
		logger.Warn("send to discard chan fail")
	}
	return nil
}
