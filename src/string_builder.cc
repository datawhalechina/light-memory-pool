#include "string_builder.h"

#include <sstream>

namespace arrow {

namespace util {
namespace detail {

StringStreamWrapper::StringStreamWrapper()
    : sstream_(std::make_unique<std::ostringstream>()), ostream_(*sstream_) {}

StringStreamWrapper::~StringStreamWrapper() {}

std::string StringStreamWrapper::str() { return sstream_->str(); }

}  // namespace detail
}  // namespace util
}  // namespace arrow