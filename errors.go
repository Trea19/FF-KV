package bitcaskminidb

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty...")
	ErrUpdateIndexFailed = errors.New("fail to update index...")
	ErrKeyNotFound       = errors.New("key is not found in database...")
	ErrDataFileNotFound  = errors.New("data file is not found...")
)
