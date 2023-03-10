package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	fio.Close()
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "test1.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("hello there!"))
	assert.Equal(t, 12, n)
	assert.Nil(t, err)

	fio.Close()
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "test2.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("aaaaa"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("bbbbb"))
	assert.Nil(t, err)

	b1 := make([]byte, 7)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 7, n)
	assert.Equal(t, "aaaaabb", string(b1))

	fio.Close()
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "test3.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)

	fio.Close()
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "test4.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)

	fio.Close()
}
