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
    - [x] todo: fix delete with multiple tables
- [x] mergeiterator
- [ ] sorted strign table (sst) encoding
- [ ] bloom filter for sst optimization
- [ ] level compaction and persistence
- [ ] write-ahead log
- [ ] _future?_

### benchmark

TODO