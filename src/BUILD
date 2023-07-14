load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "memory_pool",
    srcs = glob(["*.cc"]),
    hdrs = glob(["*.h"]),
    copts = ["-std=c++17"],
    visibility = ["//visibility:public"],
)