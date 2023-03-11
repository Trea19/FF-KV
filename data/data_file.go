package data

import "bitcask-go/fio"

type DataFile struct {
	FileId    uint32
	WriteOff  int64         // offset
	IOManager fio.IOManager // to read/write/sync/close
}

func OpenDataFile(path_dir string, file_id uint32) (*DataFile, error) {
	return nil, nil
}
