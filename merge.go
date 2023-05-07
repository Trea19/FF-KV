package bitcaskminidb

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

	// TODO 12-0250
}
