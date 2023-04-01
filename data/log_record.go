package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc type key-sz value-sz
// 4  + 1   +  5   + 5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
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

func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}
