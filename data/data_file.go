package data

import (
	"bitcask-go/fio"
	"fmt"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId    uint32
	WriteOff  int64         // offset
	IOManager fio.IOManager // to read/write/sync/close
}

func OpenDataFile(path_dir string, file_id uint32) (*DataFile, error) {
	fileName := filepath.Join(path_dir, fmt.Sprint("%09d", file_id)+DataFileNameSuffix)
	// initialize io_manager
	io_manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FileId:    file_id,
		WriteOff:  0,
		IOManager: io_manager,
	}

	return dataFile, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// read header
	headBuf, err := df.readNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}
	//decode the header
	header, headerSize := DecodeLogRecordHeader(headBuf)
	//finish reading
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	//TODO 8-2353
	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	return b, err
}
