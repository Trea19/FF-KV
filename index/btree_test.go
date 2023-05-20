package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 10})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(10), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 5})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(5), pos2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 4, Offset: 5})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
}

func TestBtree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	// case1: bt1 = nil
	it1 := NewBtreeIterator(bt1.tree, false)
	assert.Equal(t, it1.Valid(), false)

	// case2: have one log record
	bt1.Put([]byte("abcd"), &data.LogRecordPos{Fid: 1, Offset: 10})
	it2 := NewBtreeIterator(bt1.tree, false)
	assert.Equal(t, it2.Valid(), true)
	assert.NotNil(t, it2.Key())
	assert.NotNil(t, it2.Value())
	it2.Next()
	assert.Equal(t, it2.Valid(), false)

	// case3: add more log records
	bt1.Put([]byte("bcde"), &data.LogRecordPos{Fid: 1, Offset: 20})
	bt1.Put([]byte("cdef"), &data.LogRecordPos{Fid: 1, Offset: 30})
	bt1.Put([]byte("defg"), &data.LogRecordPos{Fid: 1, Offset: 40})
	it3 := NewBtreeIterator(bt1.tree, false)
	for it3.Rewind(); it3.Valid(); it3.Next() {
		assert.NotNil(t, it3.Key())
	}
	it4 := NewBtreeIterator(bt1.tree, true)
	for it4.Rewind(); it3.Valid(); it3.Next() {
		assert.NotNil(t, it3.Key())
	}

	// case4: test seek()
	it5 := NewBtreeIterator(bt1.tree, false)
	for it5.Seek([]byte("cd")); it5.Valid(); it5.Next() {
		// t.Log(it5.Key())
		assert.NotNil(t, it5.Key())
	}

	// case5: test reverse seek()
	it6 := NewBtreeIterator(bt1.tree, true)
	for it6.Seek([]byte("zz")); it6.Valid(); it6.Next() {
		// t.Log(it6.Key())
		assert.NotNil(t, it6.Key())
	}
}
