//
// Created by Madhav Shekhar Sharma on 7/13/24.
//

#ifndef ASLAM_SRC_MEMTABLE_H_
#define ASLAM_SRC_MEMTABLE_H_

#include "SkipList.h"

#include <atomic>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

class MemTable {
 private:
  SkipList<std::string, std::vector<uint8_t>> map;
  size_t _id;
  std::atomic<size_t> approxSize;
  mutable std::shared_mutex rwMutex;

 public:
  MemTable(size_t id);
  void Put(std::string_view key, std::string_view value);
  std::optional<std::vector<uint8_t>> Get(std::string_view key) const;
  void Remove(std::string_view key);
  bool Contains(std::string_view key);
  size_t Size() const;
  bool IsEmpty() const;
  void Clear();
  void PrintStructure() const;

  size_t GetId() const { return _id; }
  size_t GetApproxSize() const { return approxSize.load(std::memory_order_relaxed); }

  // wrapper over SkipList
  class Iterator {
   public:
	Iterator(typename SkipList<std::string, std::vector<uint8_t>>::Iterator it) : iter(std::move(it)) {}
	[[nodiscard]] bool IsValid() const { return iter.IsValid(); }
	void next() { iter.next(); }
	[[nodiscard]] std::string_view key() const { return iter.key(); }
	[[nodiscard]] const std::vector<uint8_t> &value() const { return iter.value(); }

   private:
	typename SkipList<std::string, std::vector<uint8_t>>::Iterator iter;
  };

  Iterator Scan(std::string_view lower, std::string_view upper) const;
  std::vector<std::pair<std::string, std::vector<uint8_t>>> GetAllEntries() const;
};

#endif//ASLAM_SRC_MEMTABLE_H_
