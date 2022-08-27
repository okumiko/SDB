package sdb

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"sdb/logger"
	"sdb/options"
)

func TestSet(t *testing.T) {
	t.Run("Standard Io", func(t *testing.T) {
		testSet(t, options.FileIO, options.BitCaskMode)
	})

	t.Run("MMap IO", func(t *testing.T) {
		testSet(t, options.MMap, options.BitCaskMode)
	})

	t.Run("Memory Mode", func(t *testing.T) {
		testSet(t, options.FileIO, options.MemoryMode)
	})
}

func testSet(t *testing.T, ioType options.IOType, mode options.StoreMode) {
	pwd, _ := os.Getwd()
	path := filepath.Join(pwd, "test/single")
	opts := options.NewDefaultOptions(path)
	opts.IoType = ioType
	opts.StoreMode = mode
	db, err := OpenDB(opts)
	assert.Nil(t, err)
	defer clearDB(db)

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

func TestGet(t *testing.T) {
	t.Run("Standard Io", func(t *testing.T) {
		testGet(t, options.FileIO, options.BitCaskMode)
	})

	t.Run("MMap IO", func(t *testing.T) {
		testGet(t, options.MMap, options.BitCaskMode)
	})

	t.Run("Memory Mode", func(t *testing.T) {
		testGet(t, options.FileIO, options.MemoryMode)
	})
}

func testGet(t *testing.T, ioType options.IOType, mode options.StoreMode) {
	pwd, _ := os.Getwd()
	path := filepath.Join(pwd, "test/single")
	opts := options.NewDefaultOptions(path)
	opts.IoType = ioType
	opts.StoreMode = mode
	db, err := OpenDB(opts)
	assert.Nil(t, err)
	defer clearDB(db)

	db.Set(nil, []byte("val-nil-key"))
	db.Set([]byte("key-1"), []byte("val-1"))
	db.Set([]byte("key-2"), []byte("val-2"))
	db.Set([]byte("key-3"), []byte("val-3"))
	db.Set([]byte("key-3"), []byte("val-3-rewrite"))

	type args struct {
		key []byte
	}
	tests := []struct {
		name    string
		db      *SDB
		args    args
		want    []byte
		wantErr bool
	}{
		{
			"nil-key", db, args{key: nil}, nil, true,
		},
		{
			"normal", db, args{key: []byte("key-1")}, []byte("val-1"), false,
		},
		{
			"normal-rewrite", db, args{key: []byte("key-3")}, []byte("val-3-rewrite"), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.db.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func clearDB(db *SDB) {
	if db != nil {
		_ = db.CloseDB()
		if err := os.RemoveAll(db.opts.DBPath); err != nil {
			logger.Errorf("clear db err: %v", err)
		}
	}
}
