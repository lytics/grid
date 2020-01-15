load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")
load("@bazel_gazelle//:def.bzl", "gazelle")

# gazelle:prefix github.com/lytics/grid
gazelle(name = "gazelle")

proto_library(
    name = "grid_proto",
    srcs = ["wire.proto"],
    visibility = ["//visibility:public"],
)

go_proto_library(
    name = "grid_go_proto",
    compilers = ["@io_bazel_rules_go//proto:go_grpc"],
    importpath = "github.com/lytics/grid",
    proto = ":grid_proto",
    visibility = ["//visibility:public"],
)

go_library(
    name = "go_default_library",
    srcs = [
        "actor.go",
        "cfg.go",
        "client.go",
        "context.go",
        "errors.go",
        "mailbox.go",
        "name.go",
        "query.go",
        "request.go",
        "server.go",
        "stack.go",
    ],
    embed = [":grid_go_proto"],
    importpath = "github.com/lytics/grid",
    visibility = ["//visibility:public"],
    deps = [
        "//codec:go_default_library",
        "//registry:go_default_library",
        "@com_github_lytics_retry//:go_default_library",
        "@io_etcd_go_etcd//clientv3:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_x_net//context:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = [
        "cfg_test.go",
        "client_test.go",
        "context_test.go",
        "name_test.go",
        "namespace_test.go",
        "query_test.go",
        "request_test.go",
        "server_test.go",
        "stack_test.go",
    ],
    embed = [":go_default_library"],
    deps = [
        "//testetcd:go_default_library",
        "@com_github_lytics_retry//:go_default_library",
        "@io_etcd_go_etcd//clientv3:go_default_library",
    ],
)
