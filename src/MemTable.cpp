//
// Created by Madhav Shekhar Sharma on 7/13/24.
//

#include "MemTable.h"
#include "SkipList.h"

#include <iostream>

/*****************************************************************************/
MemTable::MemTable(size_t id) : _id(id), approxSize(0)
/*****************************************************************************/
{
}

/*****************************************************************************/
void MemTable::Put(std::string_view key, std::string_view value)
/*****************************************************************************/
{
  std::unique_lock<std::shared_mutex> lock(rwMutex);
  std::vector<uint8_t> value_vec(value.begin(), value.end());
  map.insert(std::string(key), value_vec);
  approxSize.fetch_add(key.size() + value.size(), std::memory_order_relaxed);

  // std::cout << "Inserted key: " << key << " value: " << value << std::endl;
}

/*****************************************************************************/
GetResultPair<std::vector<uint8_t>> MemTable::Get(std::string_view key) const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(rwMutex);
  return map.get(std::string(key));
}

/*****************************************************************************/
MemTable::Iterator MemTable::Scan(std::string_view lower, std::string_view upper) const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(rwMutex);
  return {map.lowerBound(std::string(lower))};
}

/*****************************************************************************/
void MemTable::Remove(std::string_view key)
/*****************************************************************************/
{
  std::unique_lock<std::shared_mutex> lock(rwMutex);
  std::cout << "Removing key: " << key << " from MemTable " << _id << std::endl;
  map.remove(std::string(key));
}

/*****************************************************************************/
bool MemTable::Contains(std::string_view key)
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(rwMutex);
  return map.contains(std::string(key));
}

/*****************************************************************************/
size_t MemTable::Size() const {
  std::shared_lock<std::shared_mutex> lock(rwMutex);
  return map.size();
}

/*****************************************************************************/
void MemTable::Clear()
/*****************************************************************************/
{
  std::unique_lock<std::shared_mutex> lock(rwMutex);
  map.clear();
  approxSize.store(0, std::memory_order_relaxed);
}

/*****************************************************************************/
std::vector<std::pair<std::string, std::vector<uint8_t>>> MemTable::GetAllEntries() const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(rwMutex);
  return map.GetAllEntries();
}

/*****************************************************************************/
bool MemTable::IsEmpty() const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(rwMutex);
  return map.IsEmpty();
}

/*****************************************************************************/
void MemTable::PrintStructure() const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(rwMutex);
  map.printStructure();
}
