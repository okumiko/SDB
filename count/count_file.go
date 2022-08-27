package count

import (
	"encoding/binary"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"sync"

	"sdb/bitcask"
	"sdb/ioselector"
	"sdb/logger"
)

// countFile主要记录每个file的占用情况，占用高的优先merge
// countFile文件中的格式，一条记录12bytes
// +----------+-----------+-----------+
// | file__id | file_size | used_size |
// +----------+-----------+-----------+
// 0----------4-----------8-----------12

const (
	countFileRecordSize       = 12
	countFileSize       int64 = 2 << 12 // hintFile总文件大小，默认8kb
	CountFileName             = "count_file"
	CountFilePath             = "COUNT_FILE"
)

// ErrCountFileNoSpace countFile文件空间不足,按照默认一共可以统计652个文件，不会不足，说明出错了
var ErrCountFileNoSpace = errors.New("[count_file] not enough space can be allocated for count file")

// CountUpdate keyDir变更，只需要file_id和record_size信息
type CountUpdate struct {
	FileID     uint32
	RecordSize int
}

// CountFile 内存的抽象
type CountFile struct {
	sync.Mutex

	Once *sync.Once

	usedOffsets map[uint32]int64 // 已用的offset,fileID-->offset
	freeOffsets []int64          // 空闲的offset,栈结构

	selector ioselector.IOSelector

	CountRcv chan *CountUpdate // 接受keyDir的更新
}

// NewCountFile 新建countFile文件，或者打开存在的countFile
func NewCountFile(path, name string, bufferSize int) (*CountFile, error) {

	// 使用mmap()方式，因为：
	// 1.count_file文件不是顺序写，是随机写，适合mmap，不用频繁寻道
	// 2.大小固定8kb，mmap以页为单位，一般一页为4kb，最多读两页，占用内存也很小
	selector, err := ioselector.NewMMapSelector(filepath.Join(path, name), countFileSize)
	if err != nil {
		return nil, err
	}

	var (
		usedOffsets = make(map[uint32]int64)
		freeOffsets []int64
		countRcv    = make(chan *CountUpdate, bufferSize)
	)

	for offset := int64(0); ; offset += countFileRecordSize {
		buf := make([]byte, 8) // 读file_id和file_size
		if _, err = selector.Read(buf, offset); err != nil {
			if err == io.EOF || err == bitcask.ErrEndOfRecord {
				break // 读完正常退出
			}
			return nil, err
		}

		fileID := binary.LittleEndian.Uint32(buf[:4])
		fSize := binary.LittleEndian.Uint32(buf[4:8])

		if fileID == 0 && fSize == 0 { // 空闲的offset
			freeOffsets = append(freeOffsets, offset)
		} else { // 已使用的offset
			usedOffsets[fileID] = offset
		}
	}
	cf := &CountFile{
		usedOffsets: usedOffsets,
		freeOffsets: freeOffsets,
		CountRcv:    countRcv,
		selector:    selector,
		Once:        new(sync.Once),
	}
	// 启动监听协程，监听file的更新
	go cf.listenUpdate()
	return cf, nil
}

// SetFileSize 设置指定file_id的file_size,建立新文件时初始化用
func (cf *CountFile) SetFileSize(fileID uint32, fileSize uint32) error {
	cf.Lock()
	defer cf.Unlock()

	// 已经初始化过了,无需操作
	if _, ok := cf.usedOffsets[fileID]; ok {
		return nil
	}
	// 分配个offset
	offset, err := cf.alloc(fileID)
	if err != nil {
		logger.Errorf("[count_file] count file allocate err: %v", err)
		return err
	}

	// 写入file_id和file_size
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[:4], fileID)
	binary.LittleEndian.PutUint32(buf[4:8], fileSize)
	if _, err = cf.selector.Write(buf, offset); err != nil {
		logger.Errorf("[count_file] set file size err: %v", err)
		return err
	}
	return nil
}

// GetMCL == get merge candidate list
// 从count file获取需要被merge的文件
// 传入活跃文件id，不merge活跃文件，传入ratio设置的占用率阈值，超过的视为需要merge了
func (cf *CountFile) GetMCL(activeFID uint32, ratio float64) ([]uint32, error) {
	cf.Lock()
	defer cf.Unlock()

	var offset int64
	var mcl []uint32 // 待压缩文件列表
	// 读文件
	for {
		buf := make([]byte, countFileRecordSize)
		_, err := cf.selector.Read(buf, offset)
		if err != nil {
			if err == io.EOF || err == bitcask.ErrEndOfRecord {
				break
			}
			return nil, err
		}
		offset += countFileRecordSize

		fileID := binary.LittleEndian.Uint32(buf[:4])
		fileSize := binary.LittleEndian.Uint32(buf[4:8])
		usedSize := binary.LittleEndian.Uint32(buf[8:12])

		if fileSize != 0 && usedSize != 0 { // 跳过空闲的offset
			curRatio := float64(usedSize) / float64(fileSize)
			// 不是活跃文件并且占用率超过阈值
			if curRatio >= ratio && fileID != activeFID {
				mcl = append(mcl, fileID)
			}
		}
	}
	// 按file_id大小从小到大排序，file_id大小也代表创建时间的早晚
	sort.Slice(mcl, func(i, j int) bool {
		return mcl[i] < mcl[j]
	})
	return mcl, nil
}

// Sync 刷盘
func (cf *CountFile) Sync() error {
	return cf.selector.Sync()
}

func (cf *CountFile) Close() error {
	return cf.selector.Close()
}

// Clear 清理指定file_id的统计记录
func (cf *CountFile) Clear(fileID uint32) error {
	cf.Lock()
	defer cf.Unlock()

	// 没有使用的offset，无需清理
	if _, ok := cf.usedOffsets[fileID]; !ok {
		return nil
	}

	// 找到这个file_id的offset
	offset, err := cf.alloc(fileID)
	if err != nil {
		logger.Errorf("[count_file] count file allocate err: %v", err)
		return err
	}

	// 清空file_id的countFile record：写一个空的buf
	buf := make([]byte, countFileRecordSize)
	if _, err = cf.selector.Write(buf, offset); err != nil {
		logger.Errorf("[count_file] file_id %v clear err: %v", fileID, err)
		return err
	}

	// used变成free
	cf.freeOffsets = append(cf.freeOffsets, offset)
	delete(cf.usedOffsets, fileID)

	return nil
}

// alloc 给变动的file（更新/新建）分配offset，返回分配的offset。注意使用前上锁
func (cf *CountFile) alloc(fileID uint32) (int64, error) {
	// 已为该file_id分配offset，返回offset
	if offset, ok := cf.usedOffsets[fileID]; ok {
		return offset, nil
	}
	// countFile空间不足
	if len(cf.freeOffsets) == 0 {
		return 0, ErrCountFileNoSpace
	}

	// freeOffsets栈弹出
	offset := cf.freeOffsets[len(cf.freeOffsets)-1]
	cf.freeOffsets = cf.freeOffsets[:len(cf.freeOffsets)-1]
	cf.usedOffsets[fileID] = offset
	return offset, nil
}

// listenUpdate 监听keyDir更新
func (cf *CountFile) listenUpdate() {
	for { // 不停地读chan
		select {
		case countRcv, ok := <-cf.CountRcv:
			if !ok {
				if err := cf.selector.Close(); err != nil {
					logger.Errorf("close count file err: %v", err)
				}
				return
			}
			cf.updateCountFile(countRcv.FileID, countRcv.RecordSize)
		}
	}
}

// updateCountFile 统计增加file_id的占用
func (cf *CountFile) updateCountFile(fileID uint32, recordSize int) {
	if recordSize <= 0 {
		return
	}

	cf.Lock()
	defer cf.Unlock()

	// 分配一个offset
	offset, err := cf.alloc(fileID)
	if err != nil {
		logger.Errorf("[count_file] count file allocate err: %v", err)
		return
	}

	var buf []byte
	if recordSize > 0 { // 更新、新增
		// 读 used size
		buf = make([]byte, 4)
		offset += 8
		if _, err = cf.selector.Read(buf, offset); err != nil {
			logger.Errorf("[count_file] update count file err: %v", err)
			return
		}

		// used_size加上新加的record_size,
		usedSize := binary.LittleEndian.Uint32(buf)
		binary.LittleEndian.PutUint32(buf, usedSize+uint32(recordSize))
	} else { // 删除，直接清空hintFile记录
		buf = make([]byte, countFileRecordSize)
	}

	// 写如新的countFile记录
	if _, err = cf.selector.Write(buf, offset); err != nil {
		logger.Errorf("[count_file] update count file err: %v", err)
		return
	}
}
