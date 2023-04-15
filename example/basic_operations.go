package main

import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	opt := bitcask.DefalutOptions
	opt.DirPath = "/tmp/bitcask-go-example"

	db, err := bitcask.Open(opt)

	// Put
	err = db.Put([]byte("hello"), []byte("world"))
	if err != nil {
		panic(err)
	}

	// Get
	val, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("key: hello  value: %s\n", string(val))

	// Deleted
	err = db.Delete([]byte("hello"))
	if err != nil {
		panic(err)
	}
}

//TODO 9-010220
