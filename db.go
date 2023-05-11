package bitcaskminidb

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"io"
	"os"
	"path/filepath"
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
	seqNo      uint64 // id for transaction, global variable,  ++
	isMerging  bool   // if db is merging
}

// open the bitcask-db instance
func Open(options Options) (*DB, error) {
	// check the input_options
	if err := CheckOptions(options); err != nil {
		return nil, err
	}

	// if option.dir is not exist, then create it
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

	// load merge files
	// if merge-finished-file exists, replace related old data files with merged ones
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// load data files
	if err := db.LoadDataFiles(); err != nil {
		return nil, err
	}

	// load index from hint file
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

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

	// if merge has happened
	hasMerged, nonMergeFileId := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		nonMergeFid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerged = true
		nonMergeFileId = nonMergeFid
	}

	updataIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index")
		}
	}

	// cache for transaction record
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = noTransactionSeqNo

	for i, fid := range db.fileIds {
		// get data file
		var fileId = uint32(fid)

		// if hasMerged && fileId < nonMergeFileId, that means already loaded (db.loadIndexFromHintFile)
		if hasMerged && fileId < nonMergeFileId {
			continue
		}

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

			// parse log key, that includes seqNo and real key
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == noTransactionSeqNo { // not transaction
				updataIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// using write batch
				if logRecord.Type == data.LogRecordTxnFinish {
					// if transaction finish perfectly, update index
					for _, tranRecord := range transactionRecords[seqNo] {
						updataIndex(tranRecord.Record.Key, tranRecord.Record.Type, tranRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					// add log record to transactionRecords[seqNo]
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// update seqNo
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size
		}

		// if current datafile is active file, update write offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// update db.seqNo
	db.seqNo = currentSeqNo

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
		Key:   logRecordKeyWithSeq(key, noTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append log to current active data file
	pos, err := db.AppendLogRecordWithLock(log_record)
	if err != nil {
		return err
	}

	ok := db.index.Put(key, pos)
	if !ok {
		return ErrUpdateIndexFailed
	}

	return nil
}

func (db *DB) AppendLogRecordWithLock(log_record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.AppendLogRecord(log_record)
}

func (db *DB) AppendLogRecord(log_record *data.LogRecord) (*data.LogRecordPos, error) {
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

// get value according to key
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

	return db.getValueByPosition(logRecordPos)
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
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, noTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	// append to log record
	_, err := db.AppendLogRecordWithLock(logRecord)
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

// according to the logrecordPos, get the related value
func (db *DB) getValueByPosition(lrp *data.LogRecordPos) ([]byte, error) {
	// get the datafile according to the file id
	var dataFile *data.DataFile
	if db.activeFile.FileId == lrp.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[lrp.Fid]
	}

	// datafile not found
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// error when reading log record
	logRecord, _, err := dataFile.ReadLogRecord(lrp.Offset)
	if err != nil {
		return nil, err
	}

	// log record already been deleted
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// get all keys in index
func (db *DB) ListKeys() [][]byte {
	it := db.index.Iterator(false)
	defer it.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx++
	}
	return keys
}

// get all kv and perform user's specified actions(by param-fn)
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	it := db.index.Iterator(false)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		val, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}
		if !fn(it.Key(), val) {
			break // if fn return false, break
		}
	}
	return nil
}

// close database
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// close active file
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// close older files
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// sync
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}
