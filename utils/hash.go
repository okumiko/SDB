package utils

import "encoding/binary"

const MaxHashKeyHeader = 10

//EncodeHashKey key size|field size|key|field
func EncodeHashKey(key, field []byte) []byte {
	kSize := len(key)
	fSize := len(field)

	header := make([]byte, MaxHashKeyHeader)

	var index int
	index += binary.PutVarint(header[index:], int64(kSize))
	index += binary.PutVarint(header[index:], int64(fSize))

	hashKeySize := kSize + fSize
	if hashKeySize > 0 {
		buf := make([]byte, hashKeySize)
		copy(buf[:index], header)
		copy(buf[index:], key)
		copy(buf[index+kSize:], field)
		return buf
	}
	return header[:index]
}

func DecodeHashKey(key []byte) ([]byte, []byte) {
	var index int
	keySize, i := binary.Varint(key[index:])
	index += i
	_, i = binary.Varint(key[index:])
	index += i
	sep := index + int(keySize)
	return key[index:sep], key[sep:]
}
