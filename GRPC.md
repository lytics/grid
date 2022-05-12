grpc
====

When new versions of gRPC are released, the wire.proto can be
processed by running the commands below from the base of the
git repository.

```
# Make sure your protoc is version 3.1+
protoc --version

# Go get and generate.
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
go install google.golang.org/protobuf/cmd/protoc-gen-go
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative wire.proto
```
