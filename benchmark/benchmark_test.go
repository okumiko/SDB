package benchmark

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"sdb"
	"sdb/options"
)

var db *sdb.SDB

func init() {
	path := filepath.Join("/tmp", "rosedb_bench")
	opts := options.NewDefaultOptions(path)
	var err error
	db, err = sdb.OpenDB(opts)
	if err != nil {
		panic(fmt.Sprintf("open db err: %v", err))
	}
	initDataForGet()
}

func initDataForGet() {
	writeCount := 800000
	for i := 0; i < writeCount; i++ {
		err := db.Set(getKey32Bytes(i), getValue128Bytes())
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkRoseDB_Set(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Set(getKey32Bytes(i), getValue128Bytes())
		assert.Nil(b, err)
	}
}
