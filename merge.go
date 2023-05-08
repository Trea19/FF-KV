package bitcaskminidb

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
)

var MergeDirSuffix = "-merge"

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

	// todo 12-2545 new hint-finished-flag-file

	return nil
}

// merge dir level e.g. /tmp/bitcask VS /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	curDir := path.Dir(path.Clean(db.options.DirPath))
	curBase := path.Base(db.options.DirPath)
	return filepath.Join(curDir, curBase+MergeDirSuffix)
}
