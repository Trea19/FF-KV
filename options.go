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

const (
	Btree IndexerType = iota + 1
	ARtree
)

// for example
var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, //256MB
	SyncWrites:   false,
	IndexType:    Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
