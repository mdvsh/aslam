//
// Created by Madhav Shekhar Sharma on 7/21/24.
//

#ifndef ASLAM_SRC_MERGEITERATOR_H_
#define ASLAM_SRC_MERGEITERATOR_H_

#include <memory>
#include <queue>
#include <vector>

#include "LSMCommon.h"
#include "MemTable.h"

class MergeIterator {
 private:
  // should be movable but not copyable even when using uq_ptr
  struct IterWrapper {
	std::unique_ptr<MemTable::Iterator> iter;
	size_t idx;
	// move as copy del on uq_ptr
	IterWrapper(std::unique_ptr<MemTable::Iterator> _iter, size_t _idx) : iter(std::move(_iter)), idx(_idx) {}

	// move and move ctr for heap push
	IterWrapper(IterWrapper &&other) noexcept : iter(std::move(other.iter)), idx(other.idx) {}
	IterWrapper &operator=(IterWrapper &&other) noexcept {
	  if (this != &other) {
		iter = std::move(other.iter);
		idx = other.idx;
	  }
	  return *this;
	}
	// delete copy and copy assignment
	IterWrapper(const IterWrapper &) = delete;
	IterWrapper &operator=(const IterWrapper &) = delete;

	bool operator<(const IterWrapper &other) const {
	  // key based min-heap, break ties
	  return iter->key() > other.iter->key() || (iter->key() == other.iter->key() && idx > other.idx);
	}
  };

  std::vector<std::unique_ptr<MemTable::Iterator>> iters;
  std::priority_queue<IterWrapper> heap;
  // empty vector of uint8_t
  std::vector<uint8_t> emptyValue = {};

  void initHeap() {
	for (size_t i = 0; i < iters.size(); ++i) {
	  if (iters[i]->IsValid())
		heap.emplace(std::move(iters[i]), i);
	}
  }

 public:
  explicit MergeIterator(std::vector<std::unique_ptr<MemTable::Iterator>> _iters) : iters(std::move(_iters)) {
	initHeap();
  }

  void Next() {
	if (!IsValid())
	  return;

	std::string_view currentKey = heap.top().iter->key();
	while (!heap.empty() && heap.top().iter->key() == currentKey) {
	  // remember refactor we had to do to get pop, push-->emplace to work
	  IterWrapper topIterWrapper = std::move(const_cast<IterWrapper &>(heap.top()));
	  heap.pop();
	  topIterWrapper.iter->next();

	  // reorder based on validity
	  if (topIterWrapper.iter->IsValid())
		heap.emplace(std::move(topIterWrapper));
	  else
		iters[topIterWrapper.idx] = std::move(topIterWrapper.iter);
	}
  }

  [[nodiscard]] bool IsValid() const {
	return !heap.empty();
  }

  [[nodiscard]] std::string_view Key() const {
	if (!IsValid()) return {};
	return heap.top().iter->key();
  }

  [[nodiscard]] const std::vector<uint8_t> &Value() {
	if (!IsValid()) return emptyValue;
	return const_cast<std::vector<uint8_t> &>(heap.top().iter->value());
  }
};

#endif//ASLAM_SRC_MERGEITERATOR_H_
