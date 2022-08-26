package sdb

import (
	"sync"
	"time"

	"sdb/art"
	"sdb/bitcask"
	"sdb/options"
	"sdb/utils"
	"sdb/zset"
)

type (
	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}

	listIndex struct {
		mu      *sync.RWMutex
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	hashIndex struct {
		mu      *sync.RWMutex
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	setIndex struct {
		mu         *sync.RWMutex
		murmurhash *utils.Murmur128
		trees      map[string]*art.AdaptiveRadixTree
		idxTree    *art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu         *sync.RWMutex
		indexes    *zset.SortedSet
		murmurhash *utils.Murmur128
		trees      map[string]*art.AdaptiveRadixTree
		idxTree    *art.AdaptiveRadixTree
	}
)

func newStrIndex() *strIndex {
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}
func newListIndex() *listIndex {
	return &listIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newHashIndex() *hashIndex {
	return &hashIndex{trees: make(map[string]*art.AdaptiveRadixTree), mu: new(sync.RWMutex)}
}

func newSetIndex() *setIndex {
	return &setIndex{
		idxTree:    art.NewART(),
		murmurhash: utils.NewMurmur128(),
		trees:      make(map[string]*art.AdaptiveRadixTree),
		mu:         new(sync.RWMutex),
	}
}

func newZSetIndex() *zsetIndex {
	return &zsetIndex{
		indexes:    zset.New(),
		murmurhash: utils.NewMurmur128(),
		trees:      make(map[string]*art.AdaptiveRadixTree),
		mu:         new(sync.RWMutex),
	}
}

// 更新索引树
func (db *SDB) updateIndexTree(lr *bitcask.LogRecord, keyDir *keyDir, sendCount bool, dType DataType) error {

	if db.opts.StoreMode == options.MemoryMode {
		keyDir.value = lr.Value
	}

	var idxTree *art.AdaptiveRadixTree
	switch dType {
	case String:
		idxTree = db.strIndex.idxTree
	case List:
		idxTree = db.listIndex.idxTree
	case Hash:
		idxTree = db.hashIndex.idxTree
	case Set:
		idxTree = db.setIndex.idxTree
	case ZSet:
		idxTree = db.zsetIndex.idxTree
	}

	oldVal, updated := idxTree.Put(lr.Key, keyDir)
	if sendCount {
		db.sendCountChan(oldVal, updated, dType)
	}
	return nil
}

// 删除索引树指定key
func (db *SDB) deleteIndexTree(key []byte, keyDir *keyDir, dType DataType) error {
	var idxTree *art.AdaptiveRadixTree
	switch dType {
	case String:
		idxTree = db.strIndex.idxTree
	case List:
		idxTree = db.listIndex.idxTree
	case Hash:
		idxTree = db.hashIndex.idxTree
	case Set:
		idxTree = db.setIndex.idxTree
	case ZSet:
		idxTree = db.zsetIndex.idxTree
	}
	// 返回的record是被删除的record
	valDeleted, deleted := idxTree.Delete(key)
	db.sendCountChan(valDeleted, deleted, dType)
	// 还有标记这个key被删除的record没有删除，即删除标志位为1的记录本身
	// 因为已经删除了，删除的这条记录的记录也应该删除
	db.sendCountChan(keyDir, deleted, dType)
	return nil
}

// 通用取值函数
func (db *SDB) getVal(key []byte, dataType DataType) ([]byte, error) {
	// 根据key从ar树中获取keyDir
	var idxTree *art.AdaptiveRadixTree
	switch dataType {
	case String:
		idxTree = db.strIndex.idxTree
	case List:
		idxTree = db.listIndex.idxTree
	case Hash:
		idxTree = db.hashIndex.idxTree
	case Set:
		idxTree = db.setIndex.idxTree
	case ZSet:
		idxTree = db.zsetIndex.idxTree
	}

	rawValue := idxTree.Get(key)
	if rawValue == nil {
		return nil, ErrKeyNotFound
	}
	keyDir, _ := rawValue.(*keyDir)
	if keyDir == nil {
		return nil, ErrKeyNotFound
	}

	if keyDir.expiredAt != 0 && keyDir.expiredAt <= time.Now().Unix() {
		return nil, ErrKeyNotFound
	}
	// In KeyValueMemMode, the value will be stored in memory.
	// So get the value from the index info.
	if db.opts.StoreMode == options.MemoryMode && len(keyDir.value) != 0 {
		return keyDir.value, nil
	}

	// In KeyOnlyMemMode, the value not in memory, so get the value from log file at the offset.
	lf := db.getActiveLogFile(dataType)
	// 如果不在活跃文件中,从非活跃文件读，找到file_id对应的非活跃文件
	if lf.FileID != keyDir.fileID {
		lf = db.getImmutableFile(dataType, keyDir.fileID)
	}

	if lf == nil {
		return nil, ErrLogFileNotFound
	}

	// 根据offset从文件系统读record
	record, _, err := lf.ReadLogRecord(keyDir.recordOffset)
	if err != nil {
		return nil, err
	}
	// 判断是否删除或者过期
	if record.Type == bitcask.TypeDelete || (record.ExpiredAt != 0 && record.ExpiredAt < time.Now().Unix()) {
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}
