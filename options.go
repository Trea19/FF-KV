package bitcaskminidb

import "os"

type Options struct {
	DirPath      string
	DataFileSize int64       // max size of file
	SyncWrites   bool        // whether sync after write
	IndexType    IndexerType //index type: Btree/ARTree
}

type IndexerType int8

// users-iterator configuration item
type IteratorOptions struct {
	// traverse all keys prefixed with Prefix
	Prefix []byte
	// if reverse traverse, default: false
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchNum uint // the max size(num) of one batch
	SyncWrites  bool // if true, then sync after writing batch (when committing)
}

const (
	Btree IndexerType = iota + 1
	ARtree
	BPtree
)

// for example
var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, //256MB
	SyncWrites:   false,
	IndexType:    ARtree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
