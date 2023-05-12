package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(21),
		lock: new(sync.RWMutex),
	}
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
	bt.lock.Lock()

	oldItem := bt.tree.Delete(it)

	bt.lock.Unlock()
	return oldItem != nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return NewBtreeIterator(bt.tree, reverse)
}

// btree's index iterator
type btreeIterator struct {
	curIndex int     // current index number
	reverse  bool    // support reverse traversal
	values   []*Item // snapshot for the index, one Item includes key + pos
}

// new btree iterator
func NewBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// go back to the first data of iterator
func (bti *btreeIterator) Rewind() {
	bti.curIndex = 0
}

// find the first target key which is >= or <=(reverse) params-key, and start traversing from target key
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// jump to the next key
func (bti *btreeIterator) Next() {
	bti.curIndex += 1
}

// used to determine whether the traversal has been completed
func (bti *btreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

// get key of current postion
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

// get value of current positon
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

// close iterator and release resources
func (bti *btreeIterator) Close() {
	bti.values = nil
}
