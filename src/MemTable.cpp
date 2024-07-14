//
// Created by Madhav Shekhar Sharma on 7/13/24.
//

#include "MemTable.h"
#include "SkipList.h"

/*****************************************************************************/
MemTable::MemTable(size_t id) : _id(id), approx_size(0)
/*****************************************************************************/
{
}

/*****************************************************************************/
void MemTable::Put(std::string_view key, std::string_view value)
/*****************************************************************************/
{
  std::vector<uint8_t> value_vec(value.begin(), value.end());
  map.insert(std::string(key), value_vec);
  approx_size.fetch_add(key.size() + value.size(), std::memory_order_relaxed);
}

/*****************************************************************************/
std::optional<std::vector<uint8_t>> MemTable::Get(std::string_view key) const
/*****************************************************************************/
{
  return map.get(std::string(key));
}

/*****************************************************************************/
MemTable::Iterator MemTable::Scan(std::string_view lower, std::string_view upper) const
/*****************************************************************************/
{
  return {map.lowerBound(std::string(lower))};
}

/*****************************************************************************/
void MemTable::Remove(std::string_view key)
/*****************************************************************************/
{
  map.remove(std::string(key));
}

/*****************************************************************************/
bool MemTable::Contains(std::string_view key)
/*****************************************************************************/
{
  return map.contains(std::string(key));
}

/*****************************************************************************/
size_t MemTable::Size() const {
  return map.size();
}

/*****************************************************************************/
void MemTable::Clear()
/*****************************************************************************/
{
  map.clear();
  approx_size.store(0, std::memory_order_relaxed);
}

/*****************************************************************************/
std::vector<std::pair<std::string, std::vector<uint8_t>>> MemTable::GetAllEntries() const
/*****************************************************************************/
{
  return map.GetAllEntries();
}

/*****************************************************************************/
bool MemTable::IsEmpty() const
/*****************************************************************************/
{
  return map.IsEmpty();
}

/*****************************************************************************/
void MemTable::PrintStructure() const
/*****************************************************************************/
{
  map.printStructure();
}
