#include <iostream>

#include "src/memory_pool.h"

int main() {
  arrow::MemoryPool* pool = arrow::default_memory_pool();

  char* val;
  arrow::Status status = pool->Allocate(14, reinterpret_cast<uint8_t**>(&val));

  if (status.ok()) {
    std::cout << "Memory allocation successful." << std::endl;
    std::strcpy(val, "Hello, World!");
    std::cout << "Filled content: " << val << std::endl;
    pool->Free(reinterpret_cast<uint8_t*>(val), 4);
  } else {
    std::cout << "Memory allocation failed." << std::endl;
  }

  return 0;
}