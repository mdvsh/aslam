## aslam

> a concurrent log-structured merge tree storage engine in modern c++

implements:
- `Put(k, v)`
- `Get(k) -> v`
- `Delete(k)`
- `Scan(k1, k2) -> [(k, v)]`

uses level compaction, with a write-ahead log for durability

implements lock-free (templated) skiplist based memtable for fast writes.

### build

```bash
$ mkdir build
$ cd build
$ cmake ..
$ make
```

---

#### todo

- [x] memtable
  - [x] freezing and multiple tables
  - [x] concurrency
    - [x] stress test for correctness
    - [ ] todo: fix delete with multiple tables
- [ ] mergeiterator
- [ ] sorted strign table (sst) encoding
- [ ] compaction and persistence
- [ ] write-ahead log
- [ ] bloom filter
- [ ] _future?_

### benchmark

TODO