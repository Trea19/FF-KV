package bitcaskminidb

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	it := db.NewIterator(DefaultIteratorOptions)
	defer it.Close()
	assert.NotNil(t, it)
	assert.Equal(t, it.Valid(), false)
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcack-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	inputVal := utils.RandomValue(24)
	err = db.Put(utils.GetTestKey(10), inputVal)
	assert.Nil(t, err)

	it := db.NewIterator(DefaultIteratorOptions)
	defer it.Close()
	assert.NotNil(t, it)
	assert.Equal(t, it.Valid(), true)
	assert.Equal(t, it.Key(), utils.GetTestKey(10))
	val, err := it.Value()
	assert.Nil(t, err)
	assert.Equal(t, val, inputVal)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts.DirPath = dir
	db, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaa"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ccc"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bbb"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("eee"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ccd"), utils.RandomValue(10))
	assert.Nil(t, err)

	// reverse = false
	it1 := db.NewIterator(DefaultIteratorOptions)
	for it1.Rewind(); it1.Valid(); it1.Next() {
		assert.NotNil(t, it1.Key())
	}
	it1.Rewind()
	for it1.Seek([]byte("cc")); it1.Valid(); it1.Next() {
		assert.NotNil(t, it1.Key())
	}
	it1.Close()

	// reverse = true
	it2 := db.NewIterator(DefaultIteratorOptions)
	it2.opts.Reverse = true
	for it2.Rewind(); it2.Valid(); it2.Next() {
		assert.NotNil(t, it2.Key())
	}
	it2.Rewind()
	for it2.Seek([]byte("cc")); it2.Valid(); it2.Next() {
		assert.NotNil(t, it2.Key())
	}
	it2.Close()

	// prefix != nil
	it3 := db.NewIterator(DefaultIteratorOptions)
	it3.opts.Prefix = []byte("bb")
	for it3.Rewind(); it3.Valid(); it3.Next() {
		assert.NotNil(t, it3.Key())
	}
	it3.Close()
}
