package bitcaskminidb

import (
	"bitcask-go/index"
)

// for users
type Iterator struct {
	indexIter index.Iterator
	db        *DB
	opts      IteratorOptions
}

// initialize iterator
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		opts:      opts,
	}
}

// go back to the first data of iterator
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
}

// find the first target key which is >= or <=(reverse) params-key, and start traversing from target key
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
}

// jump to the next key
func (it *Iterator) Next() {
	it.indexIter.Next()
}

// used to determine whether the traversal has been completed
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// get key of current postion
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// get value of current positon
func (it *Iterator) Value() ([]byte, error) {
	lr := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	// TODO10-4754
}

// close iterator and release resources
func (it *Iterator) Close() {
	it.indexIter.Close()
}
