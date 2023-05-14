# FF-KV

**A Bitcask-based single-node KV storage engine**

- Low latency for read/write operations
- High throughput, especially when writing an incoming stream of random items (append only & write-batch)
- Handle datasets much larger than memory capacity (separate storage of memory and records)
- Quick set up after merging (hint file)
- Multiple index structures are supported (B-tree/Adaptive Radix Tree/ ... )
- Simple backup and Recovery strategy (todo)
- ...

> Here is a link to the Bitcask paper: https://riak.com/assets/bitcask-intro.pdf

## To-Do List

**User-oriented**

- [x] [Bitcask:Open(opts)] open a new or existing Bitcask datastore
- [x] [Bitcask:Get(key)] retrieve a value by key from a Bitcask datastore
- [x] [Bitcask:Put(key, value)]  store a key and value in a Bitcask datastore
- [x] [Bitcask:Delete(key)] delete a key from a Bitcask datastore
- [x] [Bitcask:ListKeys()] list all keys in a Bitcask datastore
- [x] [Bitcask:Fold(function)] fold over all K/V pairs in a Bitcask datastore
- [x] [Bitcask:Merge(dir)] merge several data files and produce hintfiles for faster startup
- [x] [Bitcask:Sync()] force any writes to sync to disk
- [x] [Bitcask:Close()] close a Bitcask datastore and flush all pending writes to disk

**Index**

- [x] use B-tree to store indexes in memory
- [x] use Adaptive Radix Tree to store indexes in memory
- [x] produce hintfiles (after merging)

**Data Files**

- [x] active file and older files
- [x] merge

**I/O Interface**


- [ ] add timestamp to a log record
- [x] checksum of log record
- [x] merge
- [ ] data backup and recovery
- [ ] 

