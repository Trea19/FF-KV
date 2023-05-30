package main

import (
	bitcack "bitcask-go"
	"fmt"
	"net/http"
	"os"
)

var db *bitcack.DB

func initDB() {
	var err error
	opts := bitcack.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	opts.DirPath = dir
	db, err = bitcack.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func main() {

	// start http server
	_ = http.ListenAndServe("localhost:8080", nil)
}
