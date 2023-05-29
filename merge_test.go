package bitcaskminidb

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// case1: empty
func TestDB_Merge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-merge-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

// case2: full of valid data
func TestDB_Merge2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-merge2")
	opts.DataFileSize = 32 * 1024 * 1024
	opts.DataFileMergeRatio = 0
	opts.DirPath = dir

	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// restart
	err = db.Close()
	assert.Nil(t, err)

	// todo : fix "removeAll in windows" bug

	// db2, err := Open(opts)
	// assert.Nil(t, err)
	// keys := db2.ListKeys()
	// assert.Equal(t, 50000, len(keys))

	// for i := 0; i < 50000; i++ {
	// 	val, err := db2.Get(utils.GetTestKey(i))
	// 	assert.Nil(t, err)
	// 	assert.NotNil(t, val)
	// }

	// db2.Close()
}
