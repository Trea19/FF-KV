package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	f, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// read data from the given pos of the file
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// write []byte into file
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// persist data
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// close file
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// get file size
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
