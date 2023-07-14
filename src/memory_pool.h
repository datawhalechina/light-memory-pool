#pragma once
#include <iostream>
#include <string>
#include <memory>
#include <atomic>
#include "visibility.h"
#include "macros.h"
#include "status.h"
namespace arrow {

namespace internal {

///////////////////////////////////////////////////////////////////////
// Helper tracking memory statistics

class MemoryPoolStats {
 public:
  MemoryPoolStats() : bytes_allocated_(0), max_memory_(0) {}

  int64_t max_memory() const { return max_memory_.load(); }

  int64_t bytes_allocated() const { return bytes_allocated_.load(); }

  inline void UpdateAllocatedBytes(int64_t diff) {
    auto allocated = bytes_allocated_.fetch_add(diff) + diff;
    // "maximum" allocated memory is ill-defined in multi-threaded code,
    // so don't try to be too rigorous here
    if (diff > 0 && allocated > max_memory_) {
      max_memory_ = allocated;
    }
  }

 protected:
  std::atomic<int64_t> bytes_allocated_;
  std::atomic<int64_t> max_memory_;
};
}  // namespace internal

/// Base class for memory allocation on the CPU.
///
/// Besides tracking the number of allocated bytes, the allocator also should
/// take care of the required 64-byte alignment.
class ARROW_EXPORT MemoryPool {
 public:
  virtual ~MemoryPool() = default;

  /// \brief EXPERIMENTAL. Create a new instance of the default MemoryPool
  static std::unique_ptr<MemoryPool> CreateDefault();

  /// Allocate a new memory region of at least size bytes.
  ///
  /// The allocated region shall be 64-byte aligned.
  virtual Status Allocate(int64_t size, uint8_t** out) = 0;

  /// Resize an already allocated memory section.
  ///
  /// As by default most default allocators on a platform don't support aligned
  /// reallocation, this function can involve a copy of the underlying data.
  virtual Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) = 0;

  /// Free an allocated region.
  ///
  /// @param buffer Pointer to the start of the allocated memory region
  /// @param size Allocated size located at buffer. An allocator implementation
  ///   may use this for tracking the amount of allocated bytes as well as for
  ///   faster deallocation if supported by its backend.
  virtual void Free(uint8_t* buffer, int64_t size) = 0;

  /// Return unused memory to the OS
  ///
  /// Only applies to allocators that hold onto unused memory.  This will be
  /// best effort, a memory pool may not implement this feature or may be
  /// unable to fulfill the request due to fragmentation.
  virtual void ReleaseUnused() {}

  /// The number of bytes that were allocated and not yet free'd through
  /// this allocator.
  virtual int64_t bytes_allocated() const = 0;

  /// Return peak memory allocation in this memory pool
  ///
  /// \return Maximum bytes allocated. If not known (or not implemented),
  /// returns -1
  virtual int64_t max_memory() const;

  /// The name of the backend used by this MemoryPool (e.g. "system" or "jemalloc").
  virtual std::string backend_name() const = 0;

 protected:
  MemoryPool() = default;
};

extern MemoryPool* default_memory_pool();
}  // namespace arrow