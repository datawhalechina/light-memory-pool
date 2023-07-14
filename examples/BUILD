load("@rules_cc//cc:defs.bzl", "cc_binary")

package(default_visibility = ["//visibility:public"])

cc_binary(
    name = "hello_world",
    srcs = ["hello_world.cc"],
    copts = ["-std=c++17"],
    deps = [
        "//src:memory_pool",
        "@catch2",
    ],
)
