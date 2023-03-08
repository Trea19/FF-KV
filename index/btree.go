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
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()

	bt.tree.ReplaceOrInsert(it)

	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)

	if btreeItem == nil {
		return nil
	}

	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Unlock()

	oldItem := bt.tree.Delete(it)

	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}

	return true
}
