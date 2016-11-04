hello
=====

The "hello world" of a grid applications.

### What You Will Learn

 1. How to start a gird application
 1. How to define a leader actor
 1. How to check for the number of peers, ie: processes
 1. How to start other actors besides the leader

### Prerequisite, Etcd

The grid library requires a V3 etcd server to be running, and this
example requires that it run on its default port number. You can
get etcd running by doing:

```sh
$ go get github.com/coreos/etcd
$ cd github.com/coreos/etcd
$ ./build
$ bin/etcd
```

### Running the Example

In a terminal run the following command from inside the hello
directory:

```sh
$ go run main.go -address localhost:7777
```

You can run as many of these processes as you want, but each
will need a different port number.