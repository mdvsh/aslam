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
	std::cout << "\nFreezing MemTable " << activeMemTable->GetId() << " with size " << activeMemTable->GetApproxSize() << std::endl;
	immutableMemTables.push_back(std::move(activeMemTable));
	activeMemTable = std::move(newMemTable);
	std::cout << "\nCreated new MemTable " << activeMemTable->GetId() << std::endl;
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
  auto res = activeMemTable->Get(key);
  if (res)
	return res;

  // reverse probe other (now) immut memTables
  for (auto it = immutableMemTables.rbegin(); it != immutableMemTables.rend(); ++it) {
	res = (*it)->Get(key);
	if (res)
	  return res;
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
