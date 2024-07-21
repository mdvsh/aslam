//
// Created by Madhav Shekhar Sharma on 7/13/24.
//

#include "LSMStore.h"
#include <iostream>

/*****************************************************************************/
LSMStore::LSMStore(size_t sizeLimit) : nextMemTableId(0), memTableSizeLimit(sizeLimit)
/*****************************************************************************/
{
  activeMemTable = std::make_unique<MemTable>(nextMemTableId++);
}

/*****************************************************************************/
void LSMStore::freezeMemTable()
/*****************************************************************************/
{
  auto newMemTable = std::make_unique<MemTable>(nextMemTableId++);
  {
	std::unique_lock<std::shared_mutex> write_lock(storeSharedMutex);
	spdlog::debug("Freezing MemTable {} with size {}", activeMemTable->GetId(), activeMemTable->GetApproxSize());
	immutableMemTables.push_back(std::move(activeMemTable));
	activeMemTable = std::move(newMemTable);
  }
  // post lock hold flush to disk (?)
}

/*****************************************************************************/
void LSMStore::Put(std::string_view key, std::string_view value)
/*****************************************************************************/
{
  bool shouldFreeze = false;
  {
	std::shared_lock<std::shared_mutex> read_lock(storeSharedMutex);
	activeMemTable->Put(key, value);
	shouldFreeze = activeMemTable->GetApproxSize() >= memTableSizeLimit;
	// spdlog::debug("k[{}] v[{}] id[{}]", key, value, activeMemTable->GetId());
  }

  if (shouldFreeze) {
	std::lock_guard<std::mutex> freeze_lock(freezeMutex);
	{
	  // check again if some other Put caused memTablelimit reaching in b/w
	  // this is, so we don't end up creating an empty memtable in another thread only to be frozen immediately
	  std::shared_lock<std::shared_mutex> read_lock(storeSharedMutex);
	  shouldFreeze = activeMemTable->GetApproxSize() >= memTableSizeLimit;
	}

	// finally,
	if (shouldFreeze)
	  freezeMemTable();
  }
}

/*****************************************************************************/
std::optional<std::vector<uint8_t>> LSMStore::Get(std::string_view key) const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> read_lock(storeSharedMutex);
  // spdlog::debug("Get key[{}] id[{}]", key, activeMemTable->GetId());

  const auto [result, value] = activeMemTable->Get(key);
  if (result == GetResult::Found)
	return value;
  else if (result == GetResult::Tombstone)
	return std::nullopt;

  // reverse probe other (now) immut memTables
  for (auto it = immutableMemTables.rbegin(); it != immutableMemTables.rend(); ++it) {
	const auto [probResult, probValue] = (*it)->Get(key);
	if (probResult == GetResult::Found)
	  return probValue;
	else if (probResult == GetResult::Tombstone)
	  return std::nullopt;
  }

  return std::nullopt;
}

/*****************************************************************************/
void LSMStore::Remove(std::string_view key)
/*****************************************************************************/
{
  bool shouldFreeze = false;
  {
	std::shared_lock<std::shared_mutex> read_lock(storeSharedMutex);
	activeMemTable->Remove(key);
	shouldFreeze = activeMemTable->GetApproxSize() >= memTableSizeLimit;
	spdlog::debug("Remove key[{}] id[{}]", key, activeMemTable->GetId());
  }

  if (shouldFreeze) {
	std::lock_guard<std::mutex> freeze_lock(freezeMutex);
	{
	  std::shared_lock<std::shared_mutex> read_lock(storeSharedMutex);
	  shouldFreeze = activeMemTable->GetApproxSize() >= memTableSizeLimit;
	}
	if (shouldFreeze)
	  freezeMemTable();
  }
}

/*****************************************************************************/
size_t LSMStore::GetImmutableMemTableCount() const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(storeSharedMutex);
  return immutableMemTables.size();
}

/*****************************************************************************/
void LSMStore::DebugPrintCurrentMemTable() const
/*****************************************************************************/
{
  std::shared_lock<std::shared_mutex> lock(storeSharedMutex);
  activeMemTable->PrintStructure();
}

/*****************************************************************************/
std::unique_ptr<MergeIterator> LSMStore::Scan(std::string_view start, std::string_view end) const
/*****************************************************************************/
{
  std::vector<std::unique_ptr<MemTable::Iterator>> iters;
  {
	std::shared_lock<std::shared_mutex> read_lock(storeSharedMutex);
	iters.push_back(std::make_unique<MemTable::Iterator>(activeMemTable->Scan(start, end)));
	for (const auto &oldMemTable : immutableMemTables)
	  iters.push_back(std::make_unique<MemTable::Iterator>(oldMemTable->Scan(start, end)));
  }
  return std::make_unique<MergeIterator>(std::move(iters));
}
