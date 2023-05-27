package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	goART "github.com/plar/go-adaptive-radix-tree"
)

// the process is almost the same as BTree
// encapsulate goART, implement interface Indexer (index.go)

type AdaptiveRadixTree struct {
	tree goART.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goART.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()

	value, isFound := art.tree.Search(key)

	if !isFound {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, isDeleted := art.tree.Delete(key)
	art.lock.Unlock()

	if oldValue == nil {
		return nil, false
	}

	return oldValue.(*data.LogRecordPos), isDeleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()

	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewARTIterator(art.tree, reverse)
}

type artIterator struct {
	curIndex int     // current index number
	reverse  bool    // support reverse traversal
	values   []*Item // snapshot for the index, one Item includes key + pos
}

func NewARTIterator(tree goART.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())

	saveValues := func(node goART.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// go back to the first data of iterator
func (arti *artIterator) Rewind() {
	arti.curIndex = 0
}

// find the first target key which is >= or <=(reverse) params-key, and start traversing from target key
func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.curIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.curIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

// jump to the next key
func (arti *artIterator) Next() {
	arti.curIndex += 1
}

// used to determine whether the traversal has been completed
func (arti *artIterator) Valid() bool {
	return arti.curIndex < len(arti.values)
}

// get key of current postion
func (arti *artIterator) Key() []byte {
	return arti.values[arti.curIndex].key
}

// get value of current positon
func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.curIndex].pos
}

// close iterator and release resources
func (arti *artIterator) Close() {
	arti.values = nil
}
