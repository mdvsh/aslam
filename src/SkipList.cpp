//
// Created by Madhav Shekhar Sharma on 7/13/24.
//

#include "SkipList.h"
#include <iostream>

template<typename K, typename V>
const V SkipList<K, V>::TOMBSTONE = V();

/*****************************************************************************/
template<typename K, typename V>
SkipList<K, V>::SkipList(int maxLevel, float prob) : maxLevel(maxLevel), p(prob), level(1), gen(std::random_device{}()), distri(0.0, 1.0)
/*****************************************************************************/
{
  K minKey{};
  head = std::make_shared<Node>(minKey, V{}, maxLevel);
}

/*****************************************************************************/
template<typename K, typename V>
void SkipList<K, V>::insert(const K &key, const V &value)
/*****************************************************************************/
{
  std::vector<std::shared_ptr<Node>> nodesToUpdate(maxLevel);
  auto curr = head;

  // start traversing from decision tree top
  for (int i = level - 1; i >= 0; --i) {
	while (curr->forward[i] && curr->forward[i]->key < key)
	  curr = curr->forward[i];
	nodesToUpdate[i] = curr;
  }

  // found >=
  curr = curr->forward[0];
  // key exists, value to update
  if (curr && curr->key == key)
	curr->value = value;
  else {
	int levelToPlaceAt = GetRandomLevel();
	if (levelToPlaceAt > level) {
	  for (int i = level; i < levelToPlaceAt; ++i)
		nodesToUpdate[i] = head;
	  level = levelToPlaceAt;
	}

	// connect list structure
	auto newNode = std::make_shared<Node>(key, value, levelToPlaceAt);
	for (int i = 0; i < levelToPlaceAt; ++i) {
	  newNode->forward[i] = nodesToUpdate[i]->forward[i];
	  nodesToUpdate[i]->forward[i] = newNode;
	}
  }
}

/*****************************************************************************/
template<typename K, typename V>
std::optional<V> SkipList<K, V>::get(const K &key) const
/*****************************************************************************/
{
  auto curr = head;
  for (int i = level - 1; i >= 0; --i)
	while (curr->forward[i] && curr->forward[i]->key < key)
	  curr = curr->forward[i];

  curr = curr->forward[0];
  if (curr && curr->key == key && curr->value != TOMBSTONE)
	return curr->value;

  return std::nullopt;
}

/*****************************************************************************/
template<typename K, typename V>
typename SkipList<K, V>::Iterator SkipList<K, V>::lowerBound(const K &key) const
/*****************************************************************************/
{
  auto curr = head;
  for (int i = level - 1; i >= 0; --i)
	while (curr->forward[i] && curr->forward[i]->key < key)
	  curr = curr->forward[i];

  return Iterator(curr->forward[0]);
}

/*****************************************************************************/
template<typename K, typename V>
void SkipList<K, V>::remove(const K &key)
/*****************************************************************************/
{
  // handle in leveled compaction
  insert(key, TOMBSTONE);
}

/*****************************************************************************/
template<typename K, typename V>
bool SkipList<K, V>::contains(const K &key) const
/*****************************************************************************/
{
  return get(key).has_value();
}

/*****************************************************************************/
template<typename K, typename V>
size_t SkipList<K, V>::size() const
/*****************************************************************************/
{
  size_t count = 0;
  for (auto it = begin(); it != end(); it.next()) {
	if (it.value() != TOMBSTONE) ++count;
  }

  return count;
}

/*****************************************************************************/
template<typename K, typename V>
bool SkipList<K, V>::IsEmpty() const
/*****************************************************************************/
{
  return (size() == 0);
}

/*****************************************************************************/
template<typename K, typename V>
void SkipList<K, V>::clear()
/*****************************************************************************/
{
  head = std::make_shared<Node>(K{}, V{}, maxLevel);
  level = 1;
}

/*****************************************************************************/
template<typename K, typename V>
std::vector<std::pair<K, V>> SkipList<K, V>::GetAllEntries() const
/*****************************************************************************/
{
  std::vector<std::pair<K, V>> entries;
  for (auto it = begin(); it != end(); it.next()) {
	if (it.value() != TOMBSTONE)
	  entries.emplace_back(it.key(), it.value());
  }
  return entries;
}

/*****************************************************************************/
template<typename K, typename V>
void SkipList<K, V>::printStructure() const
/*****************************************************************************/
{
  std::cout << "SkipList Structure:\n";
  for (int _level = this->level - 1; _level >= 0; _level--) {
	std::cout << "Level " << _level << ": ";
	auto node = head->forward[_level];
	while (node != nullptr) {
	  std::cout << "(" << node->key << ", " << node->value.size() << ") -> ";
	  node = node->forward[_level];
	}
	std::cout << "nullptr\n";
  }
  std::cout << std::endl;
}

template class SkipList<std::string, std::vector<uint8_t>>;
