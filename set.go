package sdb

import (
	"sdb/art"
	"sdb/bitcask"
)

//set和list区别：最大的不同就是List是可以重复的。而Set是不能重复的。
//set结构，无序集合
//key->val1|val2|val3|val4|...
//要解决的问题：如何判断是否重复，如果像list那样每次遍历效率太低
//思路：
//因为无序，所以磁盘中只需要存储key->mem即可，知道mem属于key的集合就ok了，无需其他信息，不像list需要维持顺序信息
//但是判断是否重复不能靠遍历读取磁盘，所以考虑要在建立内存里映射结构时添加信息
//持久化存储为了减少内存使用，即不把value直接放内存，那么可以考虑对mem做hash运算，生成唯一hash值sum，
//此时sum->mem是一一映射
//于是内存中映射可以采取一个key建立一个ar树，ar树中sum作key，keyDir还是磁盘存储的信息
//key->[sum1->keyDir1|sum2->keyDir2]
//这样O(1)查找效率，空间换时间
//缺点：查找插入删除都需要hash运算，不过与读写磁盘相比影响不大

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
		if err := db.setIndex.murmurhash.Write(mem); err != nil {
			return err
		}
		sum := db.setIndex.murmurhash.EncodeSum128()
		db.setIndex.murmurhash.Reset()

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

	if err := db.setIndex.murmurhash.Write(member); err != nil {
		return err
	}
	sum := db.setIndex.murmurhash.EncodeSum128()
	db.setIndex.murmurhash.Reset()

	entry := &bitcask.LogRecord{Key: key, Type: bitcask.TypeDelete}
	keyDir, err := db.writeLogRecord(entry, Set)
	if err != nil {
		return err
	}

	err = db.deleteIndexTree(sum, keyDir, Set)

	return nil
}
