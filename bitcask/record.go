package bitcask

import (
	"encoding/binary"
	"hash/crc32"
)

//record的头部最大长度，使用varint编码
// crc32	typ    kSize	vSize	expiredAt
//  4    +   1   +   5   +   5    +    10      = 25
const MaxHeaderSize = 25

type RecordType byte

const (
	TypeDefault RecordType = iota
	TypeDelete             //0x01表示删除数据
	TypeListSeq            //0x02表示记录的为list的seq信息
)

//record结构，被编码写入日志文件
type LogRecord struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64
	Type      RecordType
}

type RecordHeader struct {
	crc32     uint32
	typ       RecordType
	kSize     uint32
	vSize     uint32
	expiredAt int64
}

// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |---------------------RecordHeader-------------------|
//         |--------------------------crc check---------------------------|

//编码record生成字节切片
func EncodeRecord(l *LogRecord) (buf []byte, recordSize int) {
	if l == nil {
		return nil, 0
	}

	kSize := len(l.Key)
	vSize := len(l.Value)
	header := make([]byte, MaxHeaderSize)

	//crc32固定4字节

	header[4] = byte(l.Type) //typ固定1字节

	//写入header移动index，使用varint编码
	index := 5
	index += binary.PutVarint(header[index:], int64(kSize)) //binary.MaxVarintLen32,最多5字节
	index += binary.PutVarint(header[index:], int64(vSize)) //binary.MaxVarintLen32,最多5字节
	index += binary.PutVarint(header[index:], l.ExpiredAt)  //binary.MaxVarintLen64,最多10字节

	//整个record长度
	recordSize = index + kSize + vSize
	buf = make([]byte, recordSize)
	copy(buf[:index], header)
	copy(buf[index:], l.Key)
	copy(buf[index+kSize:], l.Value)

	//crc32校验
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return
}

//解码日志文件切片
func decodeHeader(buf []byte) (h *RecordHeader, index int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h = &RecordHeader{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   RecordType(buf[4]),
	}
	index = 5
	kSize, n := binary.Varint(buf[index:])
	h.kSize = uint32(kSize)
	index += int64(n)

	vSize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vSize)
	index += int64(n)

	expiredAt, n := binary.Varint(buf[index:])
	h.expiredAt = expiredAt
	index += int64(n)

	return h, index
}

func getRecordCrc(l *LogRecord, h []byte) (crc uint32) {
	if l == nil {
		return 0
	}
	crc = crc32.ChecksumIEEE(h)
	crc = crc32.Update(crc, crc32.IEEETable, l.Key)
	crc = crc32.Update(crc, crc32.IEEETable, l.Value)
	return
}
