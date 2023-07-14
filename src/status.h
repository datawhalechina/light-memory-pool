#pragma once

#include <cstring>
#include <iostream>
#include "string_builder.h"

namespace arrow {
enum class StatusCode {
  Invalid = -1,
  OK = 0,
  Cancelled = 1,
  KeyError = 2,
  CapacityError = 3,
  OutOfMemory = 4,
};

class Status {
 public:
  Status() : code_(StatusCode::OK) {}
  explicit Status(StatusCode code) : code_(code) {}
  Status(StatusCode code, const std::string& msg) : code_(code), msg_(msg) {}

  StatusCode code() const { return code_; }

  const std::string& message() const { return msg_; }

  bool ok() const { return code_ == StatusCode::OK; }

  static Status OK() { return Status(); }

  template <typename... Args>
  static Status FromArgs(StatusCode code, Args&&... args) {
    return Status(code, util::StringBuilder(std::forward<Args>(args)...));
  }

  static Status Invalid(const std::string& msg) {
    return Status(StatusCode::Invalid, msg);
  }

  /// Return an error status for invalid data (for example a string that fails parsing)
  template <typename... Args>
  static Status Invalid(Args&&... args) {
    return Status::FromArgs(StatusCode::Invalid, std::forward<Args>(args)...);
  }

  /// Return an error status when a container's capacity would exceed its limits
  template <typename... Args>
  static Status CapacityError(Args&&... args) {
    return Status::FromArgs(StatusCode::CapacityError, std::forward<Args>(args)...);
  }

  /// Return an error status for out-of-memory conditions
  template <typename... Args>
  static Status OutOfMemory(Args&&... args) {
    return Status::FromArgs(StatusCode::OutOfMemory, std::forward<Args>(args)...);
  }

  static Status Cancelled(const std::string& msg) {
    return Status(StatusCode::Cancelled, msg);
  }

  static Status KeyError(const std::string& msg) {
    return Status(StatusCode::KeyError, msg);
  }

  std::string ToString() const {
    std::string statusString;

    switch (code_) {
      case StatusCode::OK:
        statusString = "OK";
        break;
      case StatusCode::Cancelled:
        statusString = "Cancelled";
        break;
      default:
        statusString = "Unknown";
        break;
    }

    if (!msg_.empty()) {
      statusString += ": " + msg_;
    }

    return statusString;
  }

  void Abort(const std::string& message) const {
    std::cerr << "-- Arrow Fatal Error --\n";
    if (!message.empty()) {
      std::cerr << message << "\n";
    }
    std::cerr << ToString() << std::endl;
    std::abort();
  }

 private:
  StatusCode code_;
  std::string msg_;
};

#define ARROW_RETURN_IF_(condition, status, _) \
  do {                                         \
    if (ARROW_PREDICT_FALSE(condition)) {      \
      return (status);                         \
    }                                          \
  } while (0)

#define ARROW_RETURN_NOT_OK(status)                                  \
  do {                                                               \
    ARROW_RETURN_IF_(!status.ok(), status, ARROW_STRINGIFY(status)); \
  } while (false)
#define RETURN_NOT_OK(s) ARROW_RETURN_NOT_OK(s)

}  // namespace arrow