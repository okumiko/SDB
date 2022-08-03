package sdb

import (
	"sdb/bitcask"
	"sdb/count"
	"sdb/flock"
	"sdb/ioselector"
	"sdb/logger"
	"sync"
	"sync/atomic"
)

type (
	SDB struct { //db内存中的数据结构
		opts Options //设置

		//bitcask模型
		activeFiles    map[DataType]*bitcask.LogFile //活跃文件map，只有一个
		immutableFiles map[DataType]immutableFiles   //非活跃文件map，每种数据类型多个非活跃文件
		fileIDMap      map[DataType][]uint32         //仅启动时OpenDB使用，以后不更新，fid有序
		countFiles     map[DataType]*count.CountFile

		dumpState ioselector.IOSelector

		//自适应基数索引树
		strIndex  *strIndex  // String indexes
		listIndex *listIndex // List indexes.
		hashIndex *hashIndex // Hash indexes.
		setIndex  *setIndex  // Set indexes.
		zsetIndex *zsetIndex // ZSet indexes.

		mu       sync.RWMutex    //db内存结构的读写锁
		fileLock *flock.FileLock //文件锁，只允许一个进程打开文件

		closed     int32 //close状态,1表示db已经close
		mergeState int32 //merge状态，表示有正在进行merge的协程数，每种data type merge可以并发
	}

	immutableFiles map[uint32]*bitcask.LogFile //file_id与非活跃文件的映射

	//key --> keyDir
	//key --> file_id | record_size | record_offset | t_stamp
	keyDir struct {
		fileID       uint32
		recordSize   int
		recordOffset int64
		expiredAt    int64  //如果没有设置为0，表示永不过期
		value        []byte //only use in KeyValueMemMode
	}
)

//Sync 刷盘
func (db *SDB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	//非活跃文件没刷盘必要
	// 持久化所有活跃文件
	for _, activeFile := range db.activeFiles {
		if err := activeFile.Sync(); err != nil {
			return err
		}
	}
	// 持久化count file
	for _, cf := range db.countFiles {
		if err := cf.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (db *SDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	//释放文件锁
	if db.fileLock != nil {
		_ = db.fileLock.Release()
	}
	// 关闭并持久化活跃文件
	for _, activeFile := range db.activeFiles {
		_ = activeFile.Sync()
		_ = activeFile.Close()
	}
	// 关闭并持久化非活跃文件
	for _, immutableFiles := range db.immutableFiles {
		for _, file := range immutableFiles {
			_ = file.Sync()
			_ = file.Close()
		}
	}
	// 关闭并持久化count file
	for _, file := range db.countFiles {
		_ = file.Sync()
		_ = file.Close()
	}
	//设置关闭标志位
	atomic.StoreInt32(&db.closed, 1)
	return nil
}

func (db *SDB) isClosed() bool {
	return atomic.LoadInt32(&db.closed) == 1
}

//把更新消息发送给count file协程管道，通知其更新
func (db *SDB) sendCountChan(oldVal interface{}, updated bool, dataType DataType) {
	if !updated || oldVal == nil {
		return
	}
	keyDir, _ := oldVal.(*keyDir)
	if keyDir == nil || keyDir.recordSize <= 0 {
		return
	}
	countUpdate := &count.CountUpdate{
		FileID:     keyDir.fileID,
		RecordSize: keyDir.recordSize,
	}
	select {
	case db.countFiles[dataType].CountRcv <- countUpdate:
	default:
		logger.Warn("[db] send to count chan fail")
	}
}
