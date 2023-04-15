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
	// t.Log(res1)
	//header length:7, crc:4128294378
	assert.NotNil(t, res1)
	assert.Greater(t, n, int64(5))

	// value = nil
	record2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n := EncodeLogRecord(record2)
	// t.Log(res2)
	// header length:7, crc:240712713
	assert.NotNil(t, res2)
	assert.Greater(t, n, int64(5))

	// type = deleted
	record3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcack-go"),
		Type:  LogRecordDeleted,
	}
	res3, n := EncodeLogRecord(record3)
	// t.Log(res3)
	//header length:7, crc:1907756713
	assert.NotNil(t, res3)
	assert.Greater(t, n, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	// normal
	headBuf1 := []byte{234, 197, 16, 246, 0, 8, 20}
	h1, size1 := DecodeLogRecordHeader(headBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(4128294378), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	// value = nil
	headBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := DecodeLogRecordHeader(headBuf2)
	assert.NotNil(t, h2)
	// t.Log(h2, size2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	// type = deleted
	headBuf3 := []byte{169, 14, 182, 113, 1, 8, 20}
	h3, size3 := DecodeLogRecordHeader(headBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(1907756713), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	// normal
	record1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcack-go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{234, 197, 16, 246, 0, 8, 20}

	crc1 := getLogRecordCRC(record1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(4128294378), crc1)

	// value = nil
	record2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headBuf2 := []byte{9, 225, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(record2, headBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	// type = deleted
	record3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcack-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{169, 14, 182, 113, 1, 8, 20}

	crc3 := getLogRecordCRC(record3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(1907756713), crc3)
}
