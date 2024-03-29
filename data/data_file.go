package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64         // offset
	IOManager fio.IOManager // to read/write/sync/close
}

func OpenDataFile(path_dir string, file_id uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(path_dir, file_id)
	return newDataFile(fileName, file_id, ioType)
}

func OpenHintFile(path string) (*DataFile, error) {
	fileName := filepath.Join(path, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

func OpenMergeFinishedFile(path string) (*DataFile, error) {
	fileName := filepath.Join(path, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

func OpenSeqNoFile(path string) (*DataFile, error) {
	fileName := filepath.Join(path, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFIO)
}

// params: dir_path, file_id ; return: file_name
func GetDataFileName(path_dir string, file_id uint32) string {
	return filepath.Join(path_dir, fmt.Sprintf("%09d", file_id)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// initialize io_manager
	io_manager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	dataFile := &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: io_manager,
	}

	return dataFile, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// if ... , read to the end of the file
	var headerBytes int64 = maxLogRecordHeaderSize
	if headerBytes+offset > fileSize {
		headerBytes = fileSize - offset
	}
	// read header
	headBuf, err := df.readNBytes(headerBytes, offset)
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

	logRecord := &LogRecord{Type: header.recordType}
	// read key and value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
		// fmt.Printf("key:%s, value:%s", logRecord.Key, logRecord.Value)
	}

	//check crc
	crc := getLogRecordCRC(logRecord, headBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	lr := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encLogRecord, _ := EncodeLogRecord(lr)

	return df.Write(encLogRecord)
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IOManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IOManager = ioManager
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	return b, err
}
