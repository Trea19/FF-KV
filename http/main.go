package main

import (
	bitcack "bitcask-go"
	"encoding/json"
	"fmt"
	"log"
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

// post json format, decode it to kv and process
func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var kv map[string]string
	if err := json.NewDecoder(request.Body).Decode(&kv); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range kv {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put kv in db: %v", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	val, err := db.Get([]byte(key))
	if err != nil && err != bitcack.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(val))
}

func main() {
	fmt.Printf("hello")

	// sign in handler
	http.HandleFunc("bitcask/put", handlePut)
	http.HandleFunc("bitcask/get", handleGet)

	// start http server
	_ = http.ListenAndServe("localhost:8080", nil)
}
