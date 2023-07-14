#include "memory_pool.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <optional>
#include <vector>

#include "allocator.h"
#include "io_util.h"

namespace arrow {

constexpr char kDefaultBackendEnvVar[] = "ARROW_DEFAULT_MEMORY_POOL";
enum class MemoryPoolBackend : uint8_t { System };

struct SupportedBackend {
  const char* name;
  MemoryPoolBackend backend;
};

const std::vector<SupportedBackend>& SupportedBackends() {
  static std::vector<SupportedBackend> backends = {{"system", MemoryPoolBackend::System}};
  return backends;
}

std::optional<MemoryPoolBackend> UserSelectedBackend() {
  static auto user_selected_backend = []() -> std::optional<MemoryPoolBackend> {
    auto unsupported_backend = [](const std::string& name) {
      std::vector<std::string> supported;
      for (const auto backend : SupportedBackends()) {
        supported.push_back(std::string("'") + backend.name + "'");
      }
      std::cout << "Unsupported backend '" << name << "' specified in "
                << kDefaultBackendEnvVar << std::endl;
    };

    auto maybe_name = internal::GetEnvVar(kDefaultBackendEnvVar);
    if (!maybe_name.has_value()) {
      return {};
    }
    const auto name = *std::move(maybe_name);
    if (name.empty()) {
      // An empty environment variable is considered missing
      return {};
    }
    const auto found = std::find_if(
        SupportedBackends().begin(), SupportedBackends().end(),
        [&](const SupportedBackend& backend) { return name == backend.name; });
    if (found != SupportedBackends().end()) {
      return found->backend;
    }
    unsupported_backend(name);
    return {};
  }();

  return user_selected_backend;
}

int64_t MemoryPool::max_memory() const { return -1; }

template <typename Allocator>
class BaseMemoryPoolImpl : public MemoryPool {
 public:
  ~BaseMemoryPoolImpl() override {}

  Status Allocate(int64_t size, uint8_t** out) override {
    if (size < 0) {
      return Status::Invalid("negative malloc size");
    }
    if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("malloc size overflows size_t");
    }
    RETURN_NOT_OK(Allocator::AllocateAligned(size, out));
    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    if (new_size < 0) {
      return Status::Invalid("negative realloc size");
    }
    if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
      return Status::CapacityError("realloc overflows size_t");
    }
    RETURN_NOT_OK(Allocator::ReallocateAligned(old_size, new_size, ptr));
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
    Allocator::DeallocateAligned(buffer, size);

    stats_.UpdateAllocatedBytes(-size);
  }

  void ReleaseUnused() override { Allocator::ReleaseUnused(); }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  int64_t max_memory() const override { return stats_.max_memory(); }

 protected:
  internal::MemoryPoolStats stats_;
};

class SystemMemoryPool : public BaseMemoryPoolImpl<SystemAllocator> {
 public:
  std::string backend_name() const override { return "system"; }
};

MemoryPoolBackend DefaultBackend() {
  auto backend = UserSelectedBackend();
  if (backend.has_value()) {
    return backend.value();
  }
  struct SupportedBackend default_backend = SupportedBackends().front();
  return default_backend.backend;
}

std::unique_ptr<MemoryPool> MemoryPool::CreateDefault() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return std::unique_ptr<MemoryPool>(new SystemMemoryPool);
    /* FIXME: support jemalloc in the future. */
    default:
      std::cout << "Internal error: cannot create default memory pool" << std::endl;
      return nullptr;
  }
}

static struct GlobalState {
  ~GlobalState() { finalizing.store(true, std::memory_order_relaxed); }

  bool is_finalizing() const { return finalizing.load(std::memory_order_relaxed); }

  std::atomic<bool> finalizing{false};  // constructed first, destroyed last

  SystemMemoryPool system_pool;
} global_state;

MemoryPool* default_memory_pool() {
  auto backend = DefaultBackend();
  switch (backend) {
    case MemoryPoolBackend::System:
      return &global_state.system_pool;
    default:
      std::cout << "Internal error: cannot create default memory pool" << std::endl;
      return nullptr;
  }
}
}  // namespace arrow