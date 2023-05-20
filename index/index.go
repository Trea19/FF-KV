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
	Iterator(reverse bool) Iterator
	Size() int
}

type IndexType = int8

const (
	// BTree
	Btree IndexType = iota + 1
	// ARTree
	ARtree
	//B+Tree
	BPtree
)

func NewIndexer(typ IndexType, dirPath string) Indexer {
	switch typ {
	case Btree:
		return NewBtree()

	case ARtree:
		return NewART()

	case BPtree:
		return NewBPlusTree(dirPath)

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

// abstract index-iterator interface
type Iterator interface {
	Rewind()                   // go back to the first data of iterator
	Seek(key []byte)           // find the first target key which is >= or <= params-key, and start traversing from target key
	Next()                     // jump to the next key
	Valid() bool               // used to determine whether the traversal has been completed
	Key() []byte               // get key of current postion
	Value() *data.LogRecordPos // get value of current positon
	Close()                    // close iterator and release resources
}
