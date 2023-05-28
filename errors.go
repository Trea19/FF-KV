package bitcaskminidb

import "errors"

var (
	// db_instance
	ErrKeyIsEmpty        = errors.New("the key is empty...")
	ErrUpdateIndexFailed = errors.New("fail to update index...")
	ErrKeyNotFound       = errors.New("key is not found in database...")
	ErrDataFileNotFound  = errors.New("data file is not found...")
	// options
	ErrDBDirIsEmpty    = errors.New("database dir is empty")
	ErrInvalidFileSize = errors.New("database file size must be greater than 0")
	// db_dir
	ErrDataDirCorrupted = errors.New("database directory maybe corrupted")
	// batch
	ErrExceedMaxBatchNum = errors.New("exceed the max batch num")
	//merge
	ErrMergeIsInProgress = errors.New("merge is in progress, plz try it later")
	ErrInvalidMergeRatio = errors.New("invalid merge ratio, must between 0 and 1")
	//flock
	ErrDatabaseIsBeingUsed = errors.New("the database directory is used by another process")
)
