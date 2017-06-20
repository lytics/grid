Distributed Request
=======================

A Grid application that takes an http request, and
uses grid library to send request to a pool of workers
spread across servers.  This is similar to a http based
micro-service.

### What You Will Learn

How to Send/Receive messages to existing workers. Assumes
you have already read `hello` example.

### Running the Example

In a terminal run the following commands from inside the requestreply
directory:

```sh
# Start an api server
go run main.go

# Start n worker nodes
go run main.go

# Make request to worker pool
curl -sS locahost:8080/work
```