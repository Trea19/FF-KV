package bitcaskminidb

import "os"

type Options struct {
	DirPath      string
	DataFileSize int64       // max size of file
	SyncWrites   bool        // whether sync after write
	IndexType    IndexerType //index type: Btree/ARTree
}

type IndexerType int8

const (
	Btree IndexerType = iota + 1
	ARtree
)

// for example
var DefalutOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, //256MB
	SyncWrites:   false,
	IndexType:    Btree,
}
