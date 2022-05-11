module github.com/lytics/grid/v3

go 1.16

require (
	github.com/golang/protobuf v1.5.2
	github.com/lytics/retry v1.2.0
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.etcd.io/etcd/server/v3 v3.5.0
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.26.0
)

retract v3.2.4 // contains a very bad bug
