package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinish
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

// for write batch
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc         |  type       |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4 bytes       1 byte     VarLen（max:5）VarLen（max:5）  VarLen         VarLen

// encode, from log_record(struct) to []byte
// return encode log record and the length of that
func EncodeLogRecord(log_record *LogRecord) ([]byte, int64) {
	//initialize the header
	header := make([]byte, maxLogRecordHeaderSize)

	//quit crc
	//type
	header[4] = log_record.Type

	//key_sz use variable length arry
	var index = 5
	index += binary.PutVarint(header[index:], int64(len(log_record.Key)))

	//value_sz
	index += binary.PutVarint(header[index:], int64(len(log_record.Value)))

	//as we need to return the length of the record
	var recordSize = index + len(log_record.Key) + len(log_record.Value)
	//as we need to return the obj encoded log record
	//initialize the space, according to recordSize
	encBytes := make([]byte, recordSize)
	//copy header array to encBytes array
	copy(encBytes[:index], header[:index])
	//copy key to encBytes array
	copy(encBytes[index:], log_record.Key)
	//copy value to encBytes array
	copy(encBytes[index+len(log_record.Key):], log_record.Value)

	//set crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	//copy crc to encBytes array, use little endian
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// fmt.Printf("header length:%d, crc:%d", index, crc)

	return encBytes, int64(recordSize)
}

// return header and the length of header
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	//if len(buf) <= len(crc), error
	if len(buf) <= 4 {
		return nil, 0
	}
	// we need to return log_record_header(struct)
	// get crc and type
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	//get key_sz
	var index = 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)
	//get value_sz
	valueSize, n := binary.Varint(buf[index:])
	index += n
	header.valueSize = uint32(valueSize)

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	//params: logRecord, headBuf[crc32.Size:headerSize]
	//if logRecord = nil
	if lr == nil {
		return 0
	}

	// get crc of header(the input header include type, key_sz, value_sz)
	crc := crc32.ChecksumIEEE(header[:])

	// add key
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)

	// add value
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
