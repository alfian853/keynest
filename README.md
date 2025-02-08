# Keynest
Keynest (Key Nest) is a key-value store that combines in-memory with disk-based storage system known as Log-Structured merge-Tree.
This project is intended for learning purpose instead of production use.

## Features
- [x] Store key (string) and value(in any format) based on header content-type.
  - for string value, content-type:plain-text/string
  - for json value, content-type:application/json
  - for int32 or int64 value content-Type: plain-text/int32 or plain-text/int64
- [x] Thread-safe (at-least my intention) Get, Put, Delete operations.
- [x] Red-Black Tree based in-memory storage as the first layer of storage.
- [x] 2-layers disk-based storage system as the second layer of storage.
  - 1st layer: Comprises multiple files, each containing data sorted by keys. In this layer, the data order might overlap between different files.
  - 2nd layer: Also consists of multiple files with data sorted by keys, but ensures that data order is not overlapped between files.
- [x] Search optimization using:
  - Bloom filter to eliminate the file that does not contain the key.
  - Binary search to pin-point the file containing the key.
- [x] Data compaction to merge multiple files into a single file.
  - Can handle large files compaction by loading, comparing and merging data per record.
- [x] Configurable system parameters, read the `config.go`
- [x] Persistent storage
  - [x] Flush data from memory to disk based on the configured threshold.
  - [ ] Support periodical backup in-memory data to disk.  