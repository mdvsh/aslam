#include <iostream>

#include "MemTable.h"

int main() {
  std::cout << "aslam\n";

  MemTable table(1);
  table.Put("key1", "value1");
  table.Put("key2", "value2");
  table.Put("key3", "value3");

  auto value1 = table.Get("key1");
  if (value1) {
	std::cout << "Value for key1: " << std::string(value1->begin(), value1->end()) << std::endl;
  }

  for (auto it = table.Scan("key1", "key3"); it.IsValid(); it.next()) {
	std::cout << "Key: " << it.key() << ", Value: " << std::string(it.value().begin(), it.value().end()) << std::endl;
  }

  return 0;
}
