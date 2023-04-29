package bitcaskminidb

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const noTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

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

// write batch to data file and update index
func (writeBatch *WriteBatch) Commit() error {
	writeBatch.mu.Lock()
	defer writeBatch.mu.Unlock()

	// if batch is null
	if len(writeBatch.pendingWrites) == 0 {
		return nil
	}
	// if batch size is greater than MAX_NUM
	if uint(len(writeBatch.pendingWrites)) > writeBatch.opts.MaxBatchNum {
		return ErrExceedMaxBatchNum // todo: how to auto-procedure ?
	}

	// db mu for serialize commit
	writeBatch.db.mu.Lock()
	defer writeBatch.db.mu.Unlock()

	// get the newest global id for transaction
	seqNo := atomic.AddUint64(&writeBatch.db.seqNo, 1)

	// write batch to data file
	lrpos := make(map[string]*data.LogRecordPos) // to update index
	for _, lr := range writeBatch.pendingWrites {
		lrp, err := writeBatch.db.AppendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(lr.Key, seqNo), // encode key with seqNo
			Value: lr.Value,
			Type:  lr.Type,
		})
		if err != nil {
			return err
		}
		lrpos[string(lr.Key)] = lrp
	}

	// for atom, we need to add a finish-type log record to show we had finished writing to data file
	finishRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinish,
	}
	_, err := writeBatch.db.AppendLogRecord(finishRecord)
	if err != nil {
		return err
	}

	// if opts.sync == true && active file != nil, then sync
	if writeBatch.opts.SyncWrites == true && writeBatch.db.activeFile != nil {
		if err := writeBatch.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// update index
	for _, lr := range writeBatch.pendingWrites {
		pos := lrpos[string(lr.Key)]
		if lr.Type == data.LogRecordNormal {
			writeBatch.db.index.Put(lr.Key, pos)
		}
		if lr.Type == data.LogRecordDeleted {
			writeBatch.db.index.Delete(lr.Key)
		}
	}

	// clean batch
	writeBatch.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// encode log record's key with seqNo
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	seqLen := binary.PutUvarint(seq[:], seqNo)

	// seq + key
	encKey := make([]byte, len(key)+seqLen)
	copy(encKey[:seqLen], seq[:seqLen])
	copy(encKey[seqLen:], key[:])

	return encKey
}

// decode the encKey, get seqNo and key
func parseLogRecordKey(encKey []byte) ([]byte, uint64) {
	seqNo, len := binary.Uvarint(encKey)
	key := encKey[len:]
	return key, seqNo
}
