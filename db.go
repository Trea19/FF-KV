package bitcaskminidb

import (
	"bitcask-go/data"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            //current active file, append log_record
	olderFiles map[uint32]*data.DataFile //order files, read only
}

// storage engine instance
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//if the k&v is valid, than create LogRecord
	log_record := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
}

func (db *DB) AppendLogRecord(log_record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//judge whether active data file is exist
	// if nil, than initialize the active file
	if db.activeFile == nil {
		err := db.SetActiveDataFile()
		if err != nil {
			return nil, err
		}
	}

}

func (db *DB) SetActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// open new data file
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)

	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
