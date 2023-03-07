package data

// memory index, to describe the postion of log_record on disk
type LogRecordPos struct {
	Fid    uint32 //which file
	Offset int64  //where in the file
}
