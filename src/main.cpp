#include <iostream>
#include <spdlog/spdlog.h>
#include <thread>

#include "LSMStore.h"
#include "MemTable.h"
#include "MergeIterator.h"

// for concurrent reads logging
struct ReadResult {
  int key;
  bool found;
  std::string value;
};

void concurrent_writes(LSMStore &storage, int start, int end) {
  for (int i = start; i <= end; ++i) {
	storage.Put("key" + std::to_string(i), "value" + std::to_string(i));
  }
}

void concurrent_reads(const LSMStore &storage, int start, int end, std::vector<ReadResult> &results) {
  for (int i = start; i <= end; ++i) {
	auto result = storage.Get("key" + std::to_string(i));
	results.push_back({i, result.has_value(), result ? std::string(result->begin(), result->end()) : ""});
  }
}

int main() {
  std::cout << "aslam — an lsm tree storage engine...\n\n";
  spdlog::set_level(spdlog::level::debug);

  LSMStore storage(1024);// 1KB memtable size limit

  // 5 threads to write 500 k-v
  std::vector<std::thread> write_threads;
  write_threads.reserve(5);
  for (int i = 0; i < 5; ++i) {
	write_threads.emplace_back(concurrent_writes, std::ref(storage), i * 100, (i + 1) * 100 - 1);
  }
  for (auto &t : write_threads) {
	t.join();
  }

  std::cout << "Number of immutable memtables: " << storage.GetImmutableMemTableCount() << std::endl;

  storage.DebugPrintCurrentMemTable();

  // concurrently read w 5 threads those vals
  std::vector<std::vector<ReadResult>> all_results(5);
  std::vector<std::thread> read_threads;
  for (int i = 0; i < 5; ++i) {
	read_threads.emplace_back(concurrent_reads, std::ref(storage), i * 100, (i + 1) * 100 - 1, std::ref(all_results[i]));
  }

  for (auto &t : read_threads) {
	t.join();
  }

  // for (const auto &thread_results : all_results) {
  // for (const auto &result : thread_results) {
  //   if (result.found) {
  // 	std::cout << "Read key" << result.key << ": " << result.value << std::endl;
  //   } else {
  // 	std::cout << "Key" << result.key << " not found" << std::endl;
  //   }
  // }
  // }
  //
  //  // freezing count check
  //  std::cout << "Number of immutable memtables: " << storage.GetImmutableMemTableCount() << std::endl;
  //
  //  // can we remove ?

  auto value = storage.Get("key420");
  if (value) {
	std::cout << "key420 exists: " << std::string(value->begin(), value->end()) << std::endl;
  }
  storage.Remove("key420");
  auto removed_value = storage.Get("key420");
  if (removed_value) {
	std::cout << "key420 still exists: " << std::string(removed_value->begin(), removed_value->end()) << std::endl;
  } else {
	std::cout << "key420 removed successfully" << std::endl;
  }

  auto mergeIter = storage.Scan("", "key42");
  spdlog::debug("Merged Iterator scan test:\n");
  while (mergeIter->IsValid()) {
	auto val = mergeIter->Value();
	spdlog::debug("key: {} value: {}", mergeIter->Key(), std::string(val.begin(), val.end()));
	mergeIter->Next();
  }

  return 0;
}
