load("@rules_cc//cc:defs.bzl", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_test(
    name = "memory_pool_test",
    srcs = ["memory_pool_test.cc"],
    copts = ["-std=c++17"],
    deps = [
        "//src:memory_pool",
        "@catch2",
    ],
)