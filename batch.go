package bitcaskminidb

import (
	"bitcask-go/data"
	"sync"
)

type WriteBatch struct {
	opts          WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

// initialize write batch
func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		opts:          opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// put kv to batch
func (writeBatch *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	writeBatch.mu.Lock()
	defer writeBatch.mu.Unlock()

	lr := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	writeBatch.pendingWrites[string(key)] = lr
	return nil
}

// delete kv in index/batch, if key exist, add deleted-type log record
func (writeBatch *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	writeBatch.mu.Lock()
	defer writeBatch.mu.Unlock()

	lrp := writeBatch.db.index.Get(key)
	// if key is not found in index
	if lrp == nil {
		// if key exist in batch, delete it directly
		if writeBatch.pendingWrites[string(key)] != nil {
			delete(writeBatch.pendingWrites, string(key))
		}
		return nil
	}

	// append deleted-type log record
	lr := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	writeBatch.pendingWrites[string(key)] = lr
	return nil
}
