package data

import (
	"bitcask-go/fio"
	"fmt"
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

// TODO 8-1413
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}
