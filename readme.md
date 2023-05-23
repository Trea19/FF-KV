# FF-KV

**A Bitcask-based single-node KV storage engine**

- Low latency for read/write operations
- High throughput, especially when writing an incoming stream of random items (append only & write-batch)
- Handle datasets much larger than memory capacity (separate storage of index and data files)
- Faster set up after merging (hint file)
- Multiple index structures are supported (B-tree/Adaptive Radix Tree/ ... )
- Simple backup and Recovery strategy (todo)
- ...

> Here is a link to the Bitcask paper: https://riak.com/assets/bitcask-intro.pdf

## To-Do List

**User-oriented**

```go
type logRecord struct {
    Key []byte,
    Value []byte,
    Type logRecordType(LogRecordNormal/LogRecordDeleted/etc.)
}
```

- [x] options: user can set db_path/datafile_size/if_sync/index_type
- [x] [Bitcask:Open(opts)] open a new or existing Bitcask datastore
- [x] [Bitcask:Get(key)] retrieve a value by key from a Bitcask datastore
- [x] [Bitcask:Put(key, value)] store a key and value in a Bitcask datastore
- [x] [Bitcask:Delete(key)] delete a key from a Bitcask datastore
- [x] [Bitcask:Sync()] force any writes to sync to disk
- [x] [Bitcask:Close()] close a Bitcask datastore and flush all pending writes to disk
- [x] [Bitcask:ListKeys()] list all keys in a Bitcask datastore
- [x] [Bitcask:Fold(function)] fold over all K/V pairs in a Bitcask datastore
- [x] [Bitcask:Merge(dir)] merge several data files and produce hintfiles for faster startup
- [x] write batch

**Index**

```go
// index node: <key []byte, pos *logRecordPos>
// when put/delete kv, append log record to active data file before updating index

type logRecordPos struct {
    Fid uint32,
    Offset uint64,
}
```

- [x] put logRecordPos to index-node[key]
- [x] get logRecordPos by key
- [x] delete index-node[key]
- [x] iterator (specifying prefixes and reverse traversal are supported)
- [x] B Tree to store indexes in memory 
- [x] Adaptive Radix Tree to store indexes in memory
- [x] B+ Tree to store indexes on disk (encapsulates B+ tree:)
- [x] produce hintfile (after merging)
- [ ] \*Index lock granularity optimization

**Data Files**

```go
// binary format log rocord
+-----+------+----------+------------+-------+---------+
| crc | type | key size | value size |  key  |  value  |
+-----+------+----------+------------+-------+---------+
```

- [x] data files directory, active file and older files
- [x] encode struct logRecord to binary format
- [x] decode binary format back to struct logRecord
- [x] add checksum crc
- [ ] add time stamp
- [x] merge
- [ ] backup and recovery

**I/O Interface**

- [x] encapsulate standard file manipulation API (read/write/sync/close)
- [ ] use mmap when open db
- [ ] flock
- [ ] sync strategy (x bytes/sync)
- [ ] *WAL-like format (read by block)

**HTTP Interface**

to be continued :)



![fixed-all-bugs](/imgs/test5.23.png)
