//
// Created by Madhav Shekhar Sharma on 7/13/24.
//

#ifndef ASLAM_SRC_LSMSTORE_H_
#define ASLAM_SRC_LSMSTORE_H_

#include "MemTable.h"
#include <memory>
#include <mutex>
#include <shared_mutex>

class LSMStore {
 private:
  std::mutex freezeMutex;
  mutable std::shared_mutex storeSharedMutex;
  std::unique_ptr<MemTable> activeMemTable;
  std::vector<std::unique_ptr<MemTable>> immutableMemTables;// available
  size_t nextMemTableId;
  size_t memTableSizeLimit;

  void freezeMemTable();

 public:
  explicit LSMStore(size_t sizeLimit);

  void Put(std::string_view key, std::string_view value);
  [[nodiscard]] std::optional<std::vector<uint8_t>> Get(std::string_view key) const;
  void Remove(std::string_view key);
  size_t GetImmutableMemTableCount() const;

  void DebugPrintCurrentMemTable() const;
};

#endif//ASLAM_SRC_LSMSTORE_H_
