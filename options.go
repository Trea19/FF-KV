package bitcaskminidb

type Options struct {
	DirPath      string
	DataFileSize int64       // max size of file
	SyncWrites   bool        // whether sync after write
	IndexType    IndexerType //index type: BTree/ARTree
}

type IndexerType int8

const (
	Btree IndexerType = iota + 1
	ARtree
)
