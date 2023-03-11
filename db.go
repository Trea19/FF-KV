package bitcaskminidb

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            //current active file, append log_record
	olderFiles map[uint32]*data.DataFile //order files, read only
	index      index.Indexer
}

// storage engine instance
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//if the k&v is valid, than create LogRecord
	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append log to current active data file
	pos, err := db.AppendLogRecord(log_record)
	if err != nil {
		return err
	}

	ok := db.index.Put(key, pos)
	if !ok {
		return ErrUpdateIndexFailed
	}

	return nil
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

	encRecord, size := data.EncodeLogRecord(log_record)

	// if size is up to limit, change the file state
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//persist current data file to disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//active -> order files
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// set new active data file
		if err := db.SetActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// if options of syncwrites == true ...
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// under lock
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

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// get the pos value corresponding to key from memory
	logRecordPos := db.index.Get(key)

	// key not found
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// according to the pos_fid, find that file
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// if data file is not found ...
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// the record is already deleted
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}
