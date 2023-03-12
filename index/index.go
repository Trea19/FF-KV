package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// abstract index interface
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// BTree
	Btree IndexType = iota + 1
	// ARTree
	ARtree
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBtree()

	case ARtree:
		return nil //todo

	default:
		panic("unsupported index type")
	}

}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
