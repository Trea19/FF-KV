package bitcaskminidb

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	MergeDirSuffix       = "-merge"
	MergeFinishedFileKey = "merge.finished"
)

func (db *DB) Merge() error {
	// if db.activeFile = nil
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	// if db is merging
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsInProgress
	}

	// calc merge radio
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	curRatio := float32(db.reclaimSize) / float32(totalSize)
	if curRatio < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	// check available disk capacity
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	// start merging, set db.isMerge = true
	db.isMerging = true
	defer func() { db.isMerging = false }()

	// e.g. to merge file 0 1 2, file-2 is active, we need to create the new active file3
	// step 1. sync current active file
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// step2: change current active file to older file
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// step3: create new active file for users to put new datas
	if err := db.SetActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	nonMergeFileId := db.activeFile.FileId // for merge-finished-file

	// get mergeList
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	// sort mergeList
	sort.Slice(mergeFiles, func(i int, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	// get merge path
	mergePath := db.getMergePath()
	//fmt.Printf(mergePath)
	// if merge dir exists, delete it
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// create merge dir
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// create temp merge-DB-instance to merge
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false // if before completed, merge crashed ..., we can sync after merge
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// open hint file to store valid index
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return nil
	}

	// then we need to open the mergeFiles, traversal the log records, and rewrite the valid records
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			lr, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF { // finish reading current dataFile
					break
				}
				return err
			}
			// parse log record - key, and get the real key
			realKey, _ := parseLogRecordKey(lr.Key)
			// get log record pos and compare
			lrPos := db.index.Get(realKey)
			// as the valid pos in data file is the same as the one in index, so compare fileid and offset
			// if same, then record is valid
			if lrPos != nil && lrPos.Fid == dataFile.FileId && lrPos.Offset == offset {
				// clean the seqNo (if has), save space overhead
				lr.Key = logRecordKeyWithSeq(realKey, noTransactionSeqNo)
				// add the valid record to mergeDB
				pos, err := mergeDB.AppendLogRecordWithLock(lr)
				if err != nil {
					return err
				}
				// add realKey & lrPos record to hint file
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}

			offset += size
		}
	}

	// sync
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// new merge-finished-flag-file
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return nil
	}

	// write the finish lr to merge-finished-file, to mark the merged files
	mergeFinisedLogRecord := &data.LogRecord{
		Key:   []byte(MergeFinishedFileKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encMergeFinishedLogRecord, _ := data.EncodeLogRecord(mergeFinisedLogRecord)

	// the mergeFinishedFile only have one log record to record nonMergeFileId,
	// that will be used in loadMergeFiles when setting up db
	if err := mergeFinishedFile.Write(encMergeFinishedLogRecord); err != nil {
		return err
	}

	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// merge dir level e.g. /tmp/bitcask VS /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	//fmt.Printf(db.options.DirPath + "\n")
	dir := filepath.Dir(db.options.DirPath)
	//fmt.Printf(dir + "\n")
	base := filepath.Base(db.options.DirPath)
	//fmt.Printf(base + "\n")
	return filepath.Join(dir, base+MergeDirSuffix)
}

// when set up db
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()

	//fmt.Printf(mergePath + "\n")

	// if mergePath does not exist, return nil
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// first, check merge-finished-file
	var mergeFinished bool
	var mergeFileNames []string // when replacing, save cpu-time
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// if mergeFinished = false, return nil
	if !mergeFinished {
		return nil
	}

	// if mergeFinished = true, replace the orgin data files(fileId < nonMergeFileId) with the merge files
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return nil
	}

	// delete orgin data files(fileId < nonMergeFileId)
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// move merge files to data-file-dir
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		fmt.Printf(srcPath + "\n")
		dstPath := filepath.Join(db.options.DirPath, fileName)
		fmt.Printf(dstPath + "\n")
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, nil
	}

	// because only one log record, offset = 0
	record, _, err := mergeFinishFile.ReadLogRecord(0)
	if err != nil {
		return 0, nil
	}

	// get the id from record
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, nil
	}

	return uint32(nonMergeFileId), nil
}

// load index from hint file
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	// if hint file is not exitst, return nil
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// open hint file
	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}

	// load index
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)

		// err happens, include to the end of the file
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// decode pos from log record's value
		pos := data.DeCodeLogRecordPos(logRecord.Value)

		db.index.Put(logRecord.Key, pos)

		offset += size
	}
}
