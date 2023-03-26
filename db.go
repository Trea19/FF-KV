package bitcaskminidb

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // only for loading index from data files
	activeFile *data.DataFile            //current active file, append log_record
	olderFiles map[uint32]*data.DataFile //order files, read only
	index      index.Indexer
}

// open the bitcask-db instance
func Open(options Options) (*DB, error) {
	// check the input_options
	if err := CheckOptions(options); err != nil {
		return nil, err
	}

	// if option.dir does not exist, then create it
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// initialize DB struct
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(index.IndexType(options.IndexType)),
	}

	// load data files
	if err := db.LoadDataFiles(); err != nil {
		return nil, err
	}

	// TODO
	// load index of the datafiles
	if err := db.LoadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// load index from data files, iterate the records in files, and update memory index
func (db *DB) LoadIndexFromDataFiles() error {
	// if database is empty
	if len(db.fileIds) == 0 {
		return nil
	}

	for i, fid := range db.fileIds {
		// get data file
		var fileId = uint32(fid)
		var dataFile *data.DataFile

		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// get contents of current data file
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				// situation 1 : finish read
				if err == io.EOF {
					break
				}
				// others
				return err
			}

			//  construct memory index and save
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}
			offset += size
		}

		// if current datafile is active file, update write offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	return nil
}

func (db *DB) LoadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return nil
	}

	var fileIds []int
	//specify that the data file end with .data
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) { // if find the file ends with .data
			// get the file id by split filename  eg. 000001.data
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// to load files from small id to large, we need sort
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// iterate the file id, and open them
	for i, fid := range fileIds {
		datafile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		// file with the largest id is the active file, others are older files
		if i == len(fileIds)-1 {
			db.activeFile = datafile
		} else {
			db.olderFiles[uint32(fid)] = datafile
		}
	}

	return nil
}

// check input options
func CheckOptions(options Options) error {
	if options.DirPath == "" {
		return ErrDBDirIsEmpty
	}

	if options.DataFileSize <= 0 {
		return ErrInvalidFileSize
	}

	return nil
}

// storage engine instance_put
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

	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// the record is already deleted
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// whether the key is exist
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// if exist, create a delete record
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	// append to log record
	_, err := db.AppendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// delete the key from memory index
	ok := db.index.Delete(key)
	if !ok {
		return ErrUpdateIndexFailed
	}

	return nil
}
