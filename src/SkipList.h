//
// Created by Madhav Shekhar Sharma on 7/13/24.
//

#ifndef ASLAM_SRC_SKIPLIST_H_
#define ASLAM_SRC_SKIPLIST_H_

#include <memory>
#include <optional>
#include <random>
#include <string_view>
#include <vector>
#include <utility>

#include "LSMCommon.h"

template<typename K, typename V>
class SkipList {
 private:
  struct Node {
	K key;
	V value;
	std::vector<std::shared_ptr<Node>> forward;

	Node(K k, V v, int level) : key(std::move(k)), value(std::move(v)), forward(level) {}
  };

  std::shared_ptr<Node> head;
  int maxLevel;
  float p;
  int level;

  std::mt19937 gen;
  std::uniform_real_distribution<> distri;

  static const V TOMBSTONE;

  int GetRandomLevel() {
	int _level = 1;
	while (distri(gen) < p && _level < maxLevel) _level++;
	return _level;
  }

 public:
  SkipList(int maxLevel = 32, float prob = 0.5);
  void insert(const K &key, const V &value);

  [[nodiscard]] GetResultPair<V> get(const K &key) const;

  void remove(const K &key);
  bool contains(const K &key) const;

  [[nodiscard]] size_t size() const;
  [[nodiscard]] bool IsEmpty() const;
  void clear();
  void printStructure() const;

  [[nodiscard]] std::vector<std::pair<K, V>> GetAllEntries() const;

  class Iterator {
   public:
	Iterator(std::shared_ptr<Node> node) : curr(node) {}
	[[nodiscard]] bool IsValid() const { return curr != nullptr; }
	void next() { curr = curr->forward[0]; }
	const K &key() const { return curr->key; }
	const V &value() const { return curr->value; }
	bool operator!=(const Iterator &other) const { return curr != other.curr; }

   private:
	std::shared_ptr<Node> curr;
  };

  Iterator begin() const { return Iterator(head->forward[0]); }
  Iterator end() const { return Iterator(nullptr); }
  Iterator lowerBound(const K &key) const;
};

#endif//ASLAM_SRC_SKIPLIST_H_
