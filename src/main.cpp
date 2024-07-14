#include <iostream>

#include "MemTable.h"

int main() {
  std::cout << "aslam — an lsm tree storage engine...\n\n";

  MemTable table(1);
  table.Put("key1", "value1");
  table.PrintStructure();
  table.Put("key2", "value2");
  table.PrintStructure();
  table.Put("key3", "value3");
  table.PrintStructure();

  auto value1 = table.Get("key1");
  if (value1) {
	std::cout << "Value for key1: " << std::string(value1->begin(), value1->end()) << std::endl;
	table.PrintStructure();
  }

  for (auto it = table.Scan("key1", "key3"); it.IsValid(); it.next()) {
	std::cout << "Key: " << it.key() << ", Value: " << std::string(it.value().begin(), it.value().end()) << std::endl;
  }

  return 0;
}
