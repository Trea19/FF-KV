package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// memory index, to describe the postion of log_record on disk
type LogRecordPos struct {
	Fid    uint32 //which file
	Offset int64  //where in the file
}

// encode, from log_record(struct) to []byte
func EncodeLogRecord(log_record *LogRecord) ([]byte, int64) {
	return nil, 0
}
