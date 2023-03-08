package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {

}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {

}

func (bt *BTree) Delete(key []byte) bool {

}
