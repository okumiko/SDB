package sdb

import (
	"io"
	"os"
	"path/filepath"
	"sdb/art"
	"sdb/bitcask"
	"sdb/count"
	"sdb/flock"
	"sdb/logger"
	"sdb/options"
	"sdb/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func OpenDB(opts options.Options) (*SDB, error) {
	// create the dir path if not exists.
	if !utils.PathExist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// acquire file lock to prevent multiple processes from accessing the same directory.
	lockPath := filepath.Join(opts.DBPath, lockFileName)
	//加排他锁
	fileLock, err := flock.AcquireFileLock(lockPath, false)
	if err != nil {
		return nil, err
	}

	db := &SDB{
		opts: opts,

		activeFiles:    make(map[DataType]*bitcask.LogFile),
		immutableFiles: make(map[DataType]immutableFiles),

		fileLock:  fileLock,
		strIndex:  newStrIndex(),
		listIndex: newListIndex(),
		hashIndex: newHashIndex(),
		setIndex:  newSetIndex(),
		zsetIndex: newZSetIndex(),
	}

	// init discard file.
	if err := db.initCountFiles(); err != nil {
		return nil, err
	}

	// init the log files from disk.
	if err := db.initLogFiles(); err != nil {
		return nil, err
	}

	// init indexes from log files.
	if err := db.initIndexFromLogFiles(); err != nil {
		return nil, err
	}

	// 定期进行merge
	go db.regularLogFileMerge()
	return db, nil
}

func (db *SDB) initCountFiles() error {
	countFilePath := filepath.Join(db.opts.DBPath, count.CountFilePath)
	if !utils.PathExist(countFilePath) {
		if err := os.MkdirAll(countFilePath, os.ModePerm); err != nil {
			return err
		}
	}

	//每个数据类型对应一个count_file
	countFiles := make(map[DataType]*count.CountFile)
	for i := String; i < logFileTypeNum; i++ {
		name := bitcask.FileNameMap[bitcask.FileType(i)] + count.CountFileName
		hf, err := count.NewCountFile(countFilePath, name, db.opts.CountBufferSize)
		if err != nil {
			return err
		}
		countFiles[i] = hf
	}
	db.countFiles = countFiles
	return nil
}

//从磁盘加载日志文件信息
func (db *SDB) initLogFiles() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	//读sdb目录下的所有文件
	dirEntries, err := os.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}

	//根据数据类型对文件分类
	fileIDMap := make(map[DataType][]uint32)
	for _, file := range dirEntries {
		if strings.HasPrefix(file.Name(), bitcask.FilePrefix) {
			splitNames := strings.Split(file.Name(), ".")
			fID, err := strconv.Atoi(splitNames[2])
			if err != nil {
				return err
			}
			typ := DataType(bitcask.FileTypesMap[splitNames[1]])
			fileIDMap[typ] = append(fileIDMap[typ], uint32(fID))
		}
	}
	db.fileIDMap = fileIDMap

	//每个dataType都要系统调用打开文件，没必要起协程了
	for dataType, fIDs := range fileIDMap {
		db.immutableFiles[dataType] = make(immutableFiles)

		if len(fIDs) == 0 {
			continue
		}

		//把fIDs按从小到大顺序排序，fID越小代表创建越早
		sort.Slice(fIDs, func(i, j int) bool {
			return fIDs[i] < fIDs[j]
		})

		//分配给活跃和非活跃文件map
		for i, fID := range fIDs {
			fType, IOType := bitcask.FileType(dataType), bitcask.IOType(db.opts.IoType)
			lf, err := bitcask.OpenLogFile(db.opts.DBPath, fID, db.opts.LogFileSizeThreshold, fType, IOType)
			if err != nil {
				return err
			}
			// latest one is active log file.
			if i == len(fIDs)-1 {
				db.activeFiles[dataType] = lf
			} else {
				db.immutableFiles[dataType][fID] = lf
			}
		}
	}
	return nil
}

//根据日志文件构建索引树
func (db *SDB) initIndexFromLogFiles() error {
	iterateAndHandle := func(dataType DataType, wg *sync.WaitGroup) {
		defer wg.Done()

		fIDs := db.fileIDMap[dataType]
		for i, fID := range fIDs { //fIDs已经有序
			var logfile *bitcask.LogFile
			if i == len(fIDs)-1 {
				logfile = db.activeFiles[dataType]
			} else {
				logfile = db.immutableFiles[dataType][fID]
			}
			if logfile == nil {
				logger.Fatalf("log file is nil, failed to open db")
			}

			var offset int64
			for {
				record, recordSize, err := logfile.ReadLogRecord(offset)
				if err != nil {
					if err == io.EOF || err == bitcask.ErrEndOfRecord {
						break
					}
					logger.Fatalf("read log entry from file err, failed to open db")
				}
				keyDir := &keyDir{
					fileID:       fID,
					recordOffset: offset,
					recordSize:   int(recordSize),
					expiredAt:    record.ExpiredAt,
				}
				db.buildIndex(dataType, record, keyDir)
				offset += recordSize
			}
			// 设置活跃文件的写offset
			if i == len(fIDs)-1 {
				atomic.StoreInt64(&logfile.WriteOffSet, offset)
			}
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(logFileTypeNum)
	for i := 0; i < logFileTypeNum; i++ {
		go iterateAndHandle(DataType(i), wg)
	}
	wg.Wait()
	return nil
}

//key --> keyDir
//key --> file_id | record_size | record_offset | t_stamp
func (db *SDB) buildIndex(dataType DataType, record *bitcask.LogRecord, keyDir *keyDir) {
	switch dataType {
	case String:
		db.buildStrIndex(record, keyDir)
	case List:
		db.buildListIndex(record, keyDir)
	case Hash:
		db.buildHashIndex(record, keyDir)
	}
}

func (db *SDB) buildStrIndex(record *bitcask.LogRecord, keyDir *keyDir) {
	strKey := record.Key
	//过期或者删除了，删除索引
	if record.Type == bitcask.TypeDelete || (record.ExpiredAt != 0 && record.ExpiredAt < time.Now().Unix()) {
		db.strIndex.idxTree.Delete(strKey)
		return
	}
	if db.opts.IndexMode == options.KeyValueMemMode {
		keyDir.value = record.Value
	}
	db.strIndex.idxTree.Put(strKey, keyDir)
}

//两种key，
//1.seq|key
//2.key
func (db *SDB) buildListIndex(record *bitcask.LogRecord, keyDir *keyDir) {
	TreeKey := record.Key
	if record.Type != bitcask.TypeListSeq { //序号record key是key，值record key是key+seq
		TreeKey, _ = utils.DecodeListKey(record.Key)
	}
	if db.listIndex.trees[string(TreeKey)] == nil {
		db.listIndex.trees[string(TreeKey)] = art.NewART()
	}
	db.listIndex.idxTree = db.listIndex.trees[string(TreeKey)]

	//对于value来说ar树的key是key+seq
	if record.Type == bitcask.TypeDelete || (record.ExpiredAt != 0 && record.ExpiredAt < time.Now().Unix()) {
		db.listIndex.idxTree.Delete(record.Key)
		return
	}
	if db.opts.IndexMode == options.KeyValueMemMode {
		keyDir.value = record.Value
	}

	db.listIndex.idxTree.Put(record.Key, keyDir)
}

//key对应ar树，field对应每个ar树的索引
func (db *SDB) buildHashIndex(record *bitcask.LogRecord, keyDir *keyDir) {
	TreeKey, field := utils.DecodeHashKey(record.Key)
	if db.hashIndex.trees[string(TreeKey)] == nil {
		db.hashIndex.trees[string(TreeKey)] = art.NewART()
	}
	db.hashIndex.idxTree = db.hashIndex.trees[string(TreeKey)]

	if record.Type == bitcask.TypeDelete || (record.ExpiredAt != 0 && record.ExpiredAt < time.Now().Unix()) {
		db.hashIndex.idxTree.Delete(record.Key)
		return
	}

	if db.opts.IndexMode == options.KeyValueMemMode {
		keyDir.value = record.Value
	}

	db.hashIndex.idxTree.Put(field, keyDir)
}
