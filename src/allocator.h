#pragma once
#include "status.h"

namespace arrow {
// Helper class directing allocations to the standard system allocator.
class SystemAllocator {
 public:
  // Allocate memory according to the alignment requirements for Arrow
  // (as of May 2016 64 bytes)
  static Status AllocateAligned(int64_t size, uint8_t** out);

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr);

  static void DeallocateAligned(uint8_t* ptr, int64_t size);

  static void ReleaseUnused();
};
}  // namespace arrow