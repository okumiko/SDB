package benchmark

import (
	"bytes"
	"fmt"
	"math/rand"
	"time"
)

const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"

func init() {
	rand.Seed(time.Now().Unix())
}

// getKey32Bytes get一个32字节长度的byte数组
func getKey32Bytes(n int) []byte {
	return []byte("SDB-bench-key-32Bytes-" + fmt.Sprintf("%10d", n))
}

// get一个128字节长度的byte数组
func getValue128Bytes() []byte {
	var str bytes.Buffer
	for i := 0; i < 128; i++ {
		str.WriteByte(alphabet[rand.Int()%36])
	}
	return []byte(str.String())
}
