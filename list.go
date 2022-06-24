package sdb

import (
	"encoding/binary"
	"sdb/art"
	"sdb/bitcask"
	"sdb/utils"
)

//list用文件和索引树中多一个record和keyDir来记录list的headSeq和tailSeq，用key读取
//用key和seq编码listKey对应真正的值

//list允许重复
func (db *SDB) LPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	//用ar树作为list，如果key对应的list不存在则创建
	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}

	db.listIndex.idxTree = db.listIndex.trees[string(key)]
	for _, val := range values {
		if err := db.pushList(key, val, true); err != nil {
			return err
		}
	}
	return nil
}

// LPop removes and returns the first elements of the list stored at key.
func (db *SDB) LPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popList(key, true)
}

func (db *SDB) RPush(key []byte, values ...[]byte) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	//用ar树作为list，如果key对应的list不存在则创建
	if db.listIndex.trees[string(key)] == nil {
		db.listIndex.trees[string(key)] = art.NewART()
	}

	db.listIndex.idxTree = db.listIndex.trees[string(key)]
	for _, val := range values {
		if err := db.pushList(key, val, false); err != nil {
			return err
		}
	}
	return nil
}

// RPop Removes and returns the last elements of the list stored at key.
func (db *SDB) RPop(key []byte) ([]byte, error) {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	return db.popList(key, false)
}

func (db *SDB) pushList(key []byte, val []byte, isLeft bool) error {
	//根据key获取headSeq和tailSeq
	headSeq, tailSeq, err := db.getListSeq(key)
	if err != nil {
		return err
	}

	var seq = headSeq
	if !isLeft {
		seq = tailSeq
	}
	//list value对应的key由key和seq编码组成，用来访问list的left和right
	listKey := utils.EncodeListKey(key, seq)

	record := &bitcask.LogRecord{Key: listKey, Value: val}
	keyDir, err := db.writeLogRecord(record, List)
	if err != nil {
		return err
	}

	if err = db.updateIndexTree(record, keyDir, true, List); err != nil {
		return err
	}

	if isLeft { //插头
		headSeq--
	} else { //插尾
		tailSeq++
	}
	//把序号写入文件
	err = db.writeListSeq(key, headSeq, tailSeq)
	return err
}

//根据key获取list的headSeq和tailSeq
func (db *SDB) getListSeq(key []byte) (uint32, uint32, error) {
	val, err := db.getVal(key, List)
	//其他错误
	if err != nil && err != ErrKeyNotFound {
		return 0, 0, err
	}

	//新建list，初始化headSeq和tailSeq
	var headSeq uint32 = initialListSeq
	var tailSeq uint32 = initialListSeq + 1
	if len(val) != 0 {
		headSeq = binary.LittleEndian.Uint32(val[:4])
		tailSeq = binary.LittleEndian.Uint32(val[4:8])
	}
	return headSeq, tailSeq, nil
}

//文件中专门由记录，值为list的headSeq和tailSeq
func (db *SDB) writeListSeq(key []byte, headSeq, tailSeq uint32) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], headSeq)
	binary.LittleEndian.PutUint32(buf[4:8], tailSeq)
	record := &bitcask.LogRecord{Key: key, Value: buf, Type: bitcask.TypeListSeq}
	keyDir, err := db.writeLogRecord(record, List)
	if err != nil {
		return err
	}
	err = db.updateIndexTree(record, keyDir, true, List)
	return err
}

func (db *SDB) popList(key []byte, isLeft bool) ([]byte, error) {
	if db.listIndex.trees[string(key)] == nil {
		return nil, nil
	}
	db.listIndex.idxTree = db.listIndex.trees[string(key)]

	headSeq, tailSeq, err := db.getListSeq(key)
	if err != nil {
		return nil, err
	}

	size := tailSeq - headSeq - 1
	if size <= 0 { //如果list空了，重置下，防止偏移
		if headSeq != initialListSeq || tailSeq != initialListSeq+1 {
			headSeq = initialListSeq
			tailSeq = initialListSeq + 1
			err = db.writeListSeq(key, headSeq, tailSeq)
		}
		return nil, err
	}

	//获取seq，并编码listKey
	var seq = headSeq + 1
	if !isLeft {
		seq = tailSeq - 1
	}
	listKey := utils.EncodeListKey(key, seq)
	val, err := db.getVal(listKey, List)
	if err != nil {
		return nil, err
	}

	//pop还要删除
	record := &bitcask.LogRecord{Key: listKey, Type: bitcask.TypeDelete}
	keyDir, err := db.writeLogRecord(record, List)
	if err != nil {
		return nil, err
	}

	//缩短list，更新list记录
	if isLeft {
		headSeq++
	} else {
		tailSeq--
	}
	if err = db.writeListSeq(key, headSeq, tailSeq); err != nil {
		return nil, err
	}

	err = db.deleteIndexTree(key, keyDir, List)
	return val, nil
}
