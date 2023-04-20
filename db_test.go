package bitcaskminidb

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// after testing, destroy db dir
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestDB_Open(t *testing.T) {
	opt := DefalutOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opt.DirPath = dir
	db, err := Open(opt)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opt := DefalutOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	opt.DirPath = dir
	opt.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opt)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// case1: normal - put a log record
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// case2: put record of the same key
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// case3: key = nil
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// case4: value = nil
	err = db.Put(utils.GetTestKey(2), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(2))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// case5: datafile change
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// case6: restart db, and add log record
	// db.Close()  TODO!
	err = db.activeFile.Close()
	assert.Nil(t, err)
	db2, err := Open(opt)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opt := DefalutOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opt.DirPath = dir
	opt.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opt)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// case1: normal - get a log record
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// case2: get a record which the key is not exist
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// case3: get after updating value of the same key
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	tmpval, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotEqual(t, tmpval, val3)

	// case4: get after deleting
	// TODO 先10了，9还差一些测试用例 并发bug实在难顶
}