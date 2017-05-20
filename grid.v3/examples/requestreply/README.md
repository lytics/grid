Distributed Request
=======================

A Grid application that takes an http request, and
distributes forwards work to other workers, gathers results
and replies.

### What You Will Learn

 1. How to Send messages to existing workers.
 1. How to Gather replies from multiple workers.

Assumes you have already read **Hello** example.

```

### Running the Example

In a terminal run the following command from inside the hello
directory:

```sh
# Start an api server
go run main.go -api

# start n worker nodes
go run main.go

```