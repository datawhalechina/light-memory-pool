#define CATCH_CONFIG_MAIN
#include "src/memory_pool.h"

#include <iostream>

#include "catch.hpp"
#include "src/status.h"

TEST_CASE("Memory Pool Test", "[pool]") {
  WHEN("normal case") {
    auto pool = arrow::default_memory_pool();
    uint8_t* data;
    REQUIRE(pool->Allocate(100, &data).ok());
    REQUIRE(static_cast<uint64_t>(0) == reinterpret_cast<uint64_t>(data) % 64);
    REQUIRE(100 == pool->bytes_allocated());

    uint8_t* data2;
    REQUIRE(pool->Allocate(27, &data2).ok());
    REQUIRE(static_cast<uint64_t>(0) == reinterpret_cast<uint64_t>(data2) % 64);
    REQUIRE(127 == pool->bytes_allocated());

    pool->Free(data, 100);
    REQUIRE(27 ==  pool->bytes_allocated());
    pool->Free(data2, 27);
    REQUIRE(0 == pool->bytes_allocated());
  }
  WHEN("alloc oom") {
    auto pool = arrow::default_memory_pool();

    uint8_t* data;
    int64_t to_alloc = std::min<uint64_t>(std::numeric_limits<int64_t>::max(),
                                          std::numeric_limits<size_t>::max());
    // subtract 63 to prevent overflow after the size is aligned
    to_alloc -= 63;
    REQUIRE(arrow::StatusCode::OutOfMemory == pool->Allocate(to_alloc, &data).code());
  }
}