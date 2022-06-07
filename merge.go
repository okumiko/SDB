package sdb

import (
	"io"
	"os"
	"os/signal"
	"sdb/bitcask"
	"sdb/logger"
	"sdb/utils"
	"sync/atomic"
	"syscall"
	"time"
)

//手动进行指定file的merge
func (db *SDB) MergeSpecificLogFile(dataType DataType, fID int, ratio float64) error {
	if atomic.LoadInt32(&db.mergeState) > 0 {
		return ErrMergeRunning
	}
	return db.merge(dataType, fID, ratio)
}

//定期进行merge
func (db *SDB) regularLogFileMerge() {
	if db.opts.LogFileGCInterval <= 0 {
		return
	}
	//优雅关闭进程
	quitSignal := make(chan os.Signal, 1)
	signal.Notify(quitSignal, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	//开启定时器，定时进行gc
	ticker := time.NewTicker(db.opts.LogFileGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if mergeState := atomic.LoadInt32(&db.mergeState); mergeState > 0 { //有协程正在进行merge，先不开启
				logger.Warn("%v log file merge goroutines are running, skip it", mergeState)
				break
			}
			//每个dataType起个协程gc，互不干扰
			for dt := String; dt < logFileTypeNum; dt++ {
				go func(dataType DataType) {
					err := db.merge(dataType, -1, db.opts.LogFileGCRatio)
					if err != nil {
						logger.Errorf("log file gc err, dataType: [%v], err: [%v]", dataType, err)
					}
				}(dt)
			}
		case <-quitSignal:
			return
		}
	}
}

func (db *SDB) merge(dataType DataType, specifiedFid int, ratio float64) error {
	atomic.AddInt32(&db.mergeState, 1)
	defer atomic.AddInt32(&db.mergeState, -1)

	//获取活跃文件
	activeLogFile := db.getActiveLogFile(dataType)
	if activeLogFile == nil {
		return nil
	}
	if err := db.countFiles[dataType].Sync(); err != nil {
		return err
	}
	//获取可压缩文件id列表

	mcl, err := db.countFiles[dataType].GetMCL(activeLogFile.FileID, ratio)
	if err != nil {
		return err
	}

	for _, fID := range mcl {
		//如果指定文件
		if specifiedFid >= 0 && uint32(specifiedFid) != fID {
			continue
		}
		//不会压缩活跃文件，活跃文件装不下会转移为非活跃，找到这个非活跃文件
		immutableFile := db.getImmutableFile(dataType, fID)
		if immutableFile == nil {
			continue
		}

		//遍历要merge的file
		var offset int64
		for record, size, err := immutableFile.ReadLogRecord(offset); ; offset += size {
			if err != nil {
				if err == io.EOF || err == bitcask.ErrEndOfRecord {
					break //读完正常退出
				}
				return err
			}

			//删除记录的记录/过期记录跳过，不需要重写
			if record.Type == bitcask.TypeDelete || (record.ExpiredAt != 0 && record.ExpiredAt <= time.Now().Unix()) {
				continue
			}
			var rewriteErr error
			switch dataType {
			case String:
				rewriteErr = db.rewriteStr(immutableFile.FileID, offset, int(size), record)
			case List:
				rewriteErr = db.rewriteList(immutableFile.FileID, offset, int(size), record)
			case Hash:
				rewriteErr = db.rewriteHash(immutableFile.FileID, offset, int(size), record)
			}
			if rewriteErr != nil {
				return rewriteErr
			}
		}

		// 原来的fID中的数据已经全部重写到新文件中，活跃，如果满了再迁移
		db.mu.Lock()
		delete(db.immutableFiles[dataType], fID)
		_ = immutableFile.Delete()
		db.mu.Unlock()
		// 把合并后的file_id从count_file清除了
		db.countFiles[dataType].Clear(fID)
	}
	return nil
}

func (db *SDB) rewriteStr(fID uint32, offset int64, recordSize int, record *bitcask.LogRecord) error {
	//加锁，会阻碍string索引的主协程操作的，比如get，set
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	//索引树中的keyDir都是最新的，包括fID，offset，size，以这个为准
	//对于被删除的记录，因为删除操作时索引已经删除了，此时kd为nil，直接返回，不会重写
	keDir := db.strIndex.idxTree.Get(record.Key)
	if keDir == nil {
		return nil
	}
	//索引树中的keyDir都是最新的，包括fID，offset，size，以这个为准
	//对于被删除的记录，因为删除操作时索引已经删除了，此时kd为nil，直接返回，不会重写
	return db.rewrite(keDir, String, fID, offset, recordSize, record)
}
func (db *SDB) rewriteList(fID uint32, offset int64, recordSize int, record *bitcask.LogRecord) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	treeKey := record.Key
	if record.Type != bitcask.TypeListSeq {
		treeKey, _ = utils.DecodeListKey(record.Key)
	}
	if db.listIndex.trees[string(treeKey)] == nil {
		return nil
	}
	db.listIndex.idxTree = db.listIndex.trees[string(treeKey)]

	keyDir := db.listIndex.idxTree.Get(record.Key)
	if keyDir == nil {
		return nil
	}
	return db.rewrite(keyDir, List, fID, offset, recordSize, record)
}
func (db *SDB) rewriteHash(fID uint32, offset int64, recordSize int, record *bitcask.LogRecord) error {
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	treeKey, field := utils.DecodeHashKey(record.Key)

	//获取属于的ar树
	if db.listIndex.trees[string(treeKey)] == nil {
		return nil
	}
	db.listIndex.idxTree = db.listIndex.trees[string(treeKey)]

	kd := db.listIndex.idxTree.Get(field)
	if kd == nil {
		return nil
	}

	return db.rewrite(kd, Hash, fID, offset, recordSize, record)
}

func (db *SDB) rewrite(kd interface{}, dataType DataType, fID uint32, offset int64, recordSize int, record *bitcask.LogRecord) error {
	//判断是新文件(同样的fID, offset, size)以及未过期才进行重写，这里把旧文件和过期文件去掉了
	if latestKeyDir, _ := kd.(*keyDir); latestKeyDir != nil && latestKeyDir.fileID == fID &&
		latestKeyDir.recordOffset == offset && latestKeyDir.recordSize == recordSize &&
		(latestKeyDir.expiredAt == 0 || latestKeyDir.expiredAt > time.Now().Unix()) {
		// 将record重新写到活跃文件中
		newKeyDir, err := db.writeLogRecord(record, dataType)
		if err != nil {
			return err
		}
		// 更新索引树
		if err = db.updateIndexTree(record, newKeyDir, false, dataType); err != nil {
			return err
		}
	}
	return nil
}
