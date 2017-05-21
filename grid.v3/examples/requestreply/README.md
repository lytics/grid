Distributed Request
=======================

A Grid application that takes an http request, and
uses grid library to send request to a pool of workers
spread across servers.  This is similar to a http based
micro-service.

### What You Will Learn

1. How to Send/Receive messages to existing workers.


Assumes you have already read **Hello** example.

```

### Running the Example

In a terminal run the following command from inside the requestreply
directory:

```sh
# Start an api server
go run main.go

# start n worker nodes
go run main.go


curl -sS locahost:8080/work

```