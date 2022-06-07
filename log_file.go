package sdb

import (
	"sdb/bitcask"
	"sync/atomic"
)

func (db *SDB) initLogFile(dataType DataType) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFiles[dataType] != nil {
		return nil
	}
	opts := db.opts
	fileType, IOType := bitcask.FileType(dataType), bitcask.IOType(opts.IoType)
	lf, err := bitcask.OpenLogFile(opts.DBPath, bitcask.InitialLogFileId, opts.LogFileSizeThreshold, fileType, IOType)
	if err != nil {
		return
	}

	db.countFiles[dataType].SetFileSize(lf.FileID, uint32(opts.LogFileSizeThreshold))
	db.activeFiles[dataType] = lf
	return
}
func (db *SDB) getActiveLogFile(dataType DataType) (lf *bitcask.LogFile) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeFiles[dataType]
}

// 把record写入文件，返回keyDir，内存中应存的信息
func (db *SDB) writeLogRecord(lr *bitcask.LogRecord, dataType DataType) (kd *keyDir, err error) {
	if err = db.initLogFile(dataType); err != nil {
		return
	}

	//获取磁盘活跃文件抽象
	activeFile := db.getActiveLogFile(dataType)
	if activeFile == nil {
		return nil, ErrLogFileNotFound
	}

	opts := db.opts
	//编码record
	lrBuf, recordSize := bitcask.EncodeRecord(lr)

	//超过设定每个日志文件大小阈值，把活跃日志文件设置为非活跃文件
	if activeFile.WriteOffSet+int64(recordSize) > opts.LogFileSizeThreshold {

		//先把活跃文件刷盘
		if err = activeFile.Sync(); err != nil {
			return
		}

		//加锁，防止:1.file_map冲突 2.file_id冲突 3 file_map单实例
		db.mu.Lock()
		defer db.mu.Unlock()

		// 老活跃文件视为immutableFiles，转移下内存中的映射关系
		activeFileId := activeFile.FileID
		if db.immutableFiles[dataType] == nil {
			db.immutableFiles[dataType] = make(immutableFiles)
		}
		db.immutableFiles[dataType][activeFileId] = activeFile

		// 打开一个新日志文件，来作为新的活跃日志文件
		var lf *bitcask.LogFile
		fType, IOType := bitcask.FileType(dataType), bitcask.IOType(opts.IoType)
		lf, err = bitcask.OpenLogFile(opts.DBPath, activeFileId+1, opts.LogFileSizeThreshold, fType, IOType)
		if err != nil {
			return
		}
		//新日志文件，初始化下他在count file中的记录
		db.countFiles[dataType].SetFileSize(lf.FileID, uint32(opts.LogFileSizeThreshold))
		//活跃文件映射替换为新文件
		db.activeFiles[dataType] = lf
		activeFile = lf
	}

	// 追加写文件
	if err = activeFile.Write(lrBuf); err != nil {
		return
	}
	// 是否立刻持久化，根据设置
	if opts.Sync {
		if err = activeFile.Sync(); err != nil {
			return
		}
	}
	kd = &keyDir{
		fileID:       activeFile.FileID,
		recordSize:   recordSize,
		recordOffset: atomic.LoadInt64(&activeFile.WriteOffSet),
		expiredAt:    lr.ExpiredAt,
	}
	return
}

func (db *SDB) getImmutableFile(dataType DataType, fid uint32) (lf *bitcask.LogFile) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.immutableFiles[dataType] != nil {
		lf = db.immutableFiles[dataType][fid]
	}
	return
}
