package sdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"sdb/options"
)

func TestSDB_Set(t *testing.T) {
	t.Run("Standard Io", func(t *testing.T) {
		testRoseDBSet(t, options.FileIO, options.BitCaskMode)
	})

	t.Run("MMap IO", func(t *testing.T) {
		testRoseDBSet(t, options.MMap, options.BitCaskMode)
	})

	t.Run("Memory Mode", func(t *testing.T) {
		testRoseDBSet(t, options.FileIO, options.MemoryMode)
	})
}

func testRoseDBSet(t *testing.T, ioType options.IOType, mode options.StoreMode) {
	path := filepath.Join("/tmp", "SDB")
	opts := options.NewDefaultOptions(path)
	opts.IoType = ioType
	opts.StoreMode = mode
	db, err := OpenDB(opts)
	assert.Nil(t, err)
	defer func() {
		_ = db.CloseDB()
		_ = os.RemoveAll(db.opts.DBPath)
	}()

	type args struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name    string
		db      *SDB
		args    args
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil, value: []byte("val-nil-key")}, false,
		},
		{
			"nil-value", db, args{key: []byte("key-nil-value"), value: nil}, false,
		},
		{
			"normal", db, args{key: []byte("key-normal"), value: []byte("value-normal")}, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.db.Set(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
