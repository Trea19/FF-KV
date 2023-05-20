package index

import (
	"bitcask-go/data"
	"path/filepath"

	"go.etcd.io/bbolt"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string) *BPlusTree {
	// todo 13-10003
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	if err != nil {
		panic("failed to open bptree")
	}

	// create bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		val := bucket.Get(key)
		if len(val) != 0 {
			pos = data.DeCodeLogRecordPos(val)
		}
		return nil
	}); err != nil {
		panic("failed to get value from bptree")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool = false
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		val := bucket.Get(key)
		if len(val) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete kv in bptree")
	}
	return ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPlusTreeIterator(bpt.tree, reverse)
}

type bptreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	reverse bool
	curKey  []byte
	curVal  []byte
}

func newBPlusTreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction in bptreeIterator")
	}
	bpti := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpti.Rewind()

	return bpti
}

// go back to the first data of iterator
func (bpti *bptreeIterator) Rewind() {
	if bpti.reverse {
		bpti.curKey, bpti.curVal = bpti.cursor.Last()
	} else {
		bpti.curKey, bpti.curVal = bpti.cursor.First()
	}
}

// find the first target key which is >= or <= params-key, and start traversing from target key
func (bpti *bptreeIterator) Seek(key []byte) {
	bpti.curKey, bpti.curVal = bpti.cursor.Seek(key)
}

// jump to the next key
func (bpti *bptreeIterator) Next() {
	if bpti.reverse {
		bpti.curKey, bpti.curVal = bpti.cursor.Prev()
	} else {
		bpti.curKey, bpti.curVal = bpti.cursor.Next()
	}
}

// used to determine whether the traversal has been completed
func (bpti *bptreeIterator) Valid() bool {
	return len(bpti.curKey) != 0
}

// get key of current postion
func (bpti *bptreeIterator) Key() []byte {
	return bpti.curKey
}

// get value of current positon
func (bpti *bptreeIterator) Value() *data.LogRecordPos {
	return data.DeCodeLogRecordPos(bpti.curVal)
}

// close iterator and release resources
func (bpti *bptreeIterator) Close() {
	_ = bpti.tx.Rollback()
}