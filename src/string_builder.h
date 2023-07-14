#pragma once

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "visibility.h"

namespace arrow {
namespace util {
namespace detail {
class ARROW_EXPORT StringStreamWrapper {
 public:
  StringStreamWrapper();
  ~StringStreamWrapper();

  std::ostream& stream() { return ostream_; }
  std::string str();

 protected:
  std::unique_ptr<std::ostringstream> sstream_;
  std::ostream& ostream_;
};

}  // namespace detail

template <typename Head>
void StringBuilderRecursive(std::ostream& stream, Head&& head) {
  stream << head;
}

template <typename Head, typename... Tail>
void StringBuilderRecursive(std::ostream& stream, Head&& head, Tail&&... tail) {
  StringBuilderRecursive(stream, std::forward<Head>(head));
  StringBuilderRecursive(stream, std::forward<Tail>(tail)...);
}

template <typename... Args>
std::string StringBuilder(Args&&... args) {
  detail::StringStreamWrapper ss;
  StringBuilderRecursive(ss.stream(), std::forward<Args>(args)...);
  return ss.str();
}
}  // namespace util
}  // namespace arrow