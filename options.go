package bitcaskminidb

type Options struct {
	DirPath      string
	DataFileSize int64 // max size of file
	SyncWrites   bool  // whether sync after write
}
