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
