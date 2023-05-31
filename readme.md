# FF-KV

FF-KV is a bitcask-based single-node key-value storage engine.

## Features

- Keys and values are arbitrary byte arrays.
- The basic kv operations are `Put(key,value)`, `Get(key)`, `Delete(key)`, `ListKeys()`, `Fold(function)`, `Write Batch`.
- The basic engine operations are `Open(options)`, `Close()`, `Sync()`, `Merge(dir)`, `Stat()`.
- Multiple indexes are supported (B Tree/Adaptive Radix Tree/ B+ Tree)
- Forward and backward iteration is supported over the data.
- Checksum is supported.
- HTTP interface is supported.
- Backup and recovery strategy is simple.
- Users can convert I/O type to mmap when starting FF-KV.
- Handle datasets much larger than memory capacity.

## Limitations

- This is not a SQL database. It does not have a relational data model, it does not support SQL queries.
- Only a single process (possibly multi-threaded) can access a particular database at a time.

## Usage

**Case-1: Using HTTP Interface**

Step-1: Start HTTP server.

![http-1](/imgs/http1-setup.png)

Step-2: Send HTTP requests (open a new cmd window).
 `HandlePut`, `HandleGet`, `HandleDelete`, `HandleListKeys`, `HandleStat`  are supported.

![http-2](/imgs/http2-curl.png)

**Case-2: Embedded in Other Programs**

Please refer to `/example/basic_operation.go` .  // todo, not completed yet :(

## Performance

// todo

![fixed-all-bugs](/imgs/test5.23.png)

## Optimization List

- [ ] Index lock granularity optimization

- [ ] MVCC (batch commit)

- [ ] WAL-like format (read by block)
- [ ] ...

## Link

bitcask-intro: https://riak.com/assets/bitcask-intro.pdf
