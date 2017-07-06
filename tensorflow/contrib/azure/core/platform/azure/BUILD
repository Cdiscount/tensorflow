# Description:
# Azure file system implementation.

package(
    default_visibility = ["//visibility:public"],
)

licenses(["notice"])  # Apache 2.0

load(
    "//tensorflow/core/platform/azure:azure.bzl",
    "tf_cc_test",
    "tf_copts",
)

filegroup(
    name = "all_files",
    srcs = glob(
        ["**/*"],
        exclude = [
            "**/METADATA",
            "**/OWNERS",
        ],
    ),
    visibility = ["//tensorflow:__subpackages__"],
)


cc_library(
    name = "azure_file_system",
    srcs = ["azure_file_system.cc"],
    hdrs = ["azure_file_system.h"],
    deps = [
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:framework",
    ],
    linkopts = ["-lcrypto", "-lssl", "-lboost_system", "-lazurestorage", "-lcpprest", "-L/usr/local/lib/"],
    alwayslink = 1,
    copts = tf_copts(),
    visibility = ["//visibility:public"],
)

tf_cc_test(
    name = "azure_file_system_test",
    size = "small",
    srcs = [
        "azure_file_system_test.cc",
    ],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        ":azure_file_system",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)
