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
)
