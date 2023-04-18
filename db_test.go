package bitcaskminidb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// after testing, destroy db dir
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opt := DefalutOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opt.DirPath = dir
	db, err := Open(opt)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

//TODO 9-Put
