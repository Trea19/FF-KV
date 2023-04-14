package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// normal
	record1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcack-go"),
		Type:  LogRecordNormal,
	}
	res1, n := EncodeLogRecord(record1)
	t.Log(res1)
	//header length:7, crc:4128294378
	assert.NotNil(t, res1)
	assert.Greater(t, n, int64(5))
	// value = nil

	// type = deleted

}

func TestDecodeLogRecordHeader(t *testing.T) {
	headBuf1 := []byte{234, 197, 16, 246, 0, 8, 20}
	h1, size1 := DecodeLogRecordHeader(headBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(4128294378), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

}

func TestGetLogRecordCRC(t *testing.T) {
	record1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcack-go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{234, 197, 16, 246, 0, 8, 20}

	crc1 := getLogRecordCRC(record1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(4128294378), crc1)

}

//TODO 34-28
