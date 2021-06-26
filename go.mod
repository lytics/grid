module github.com/lytics/grid/v3

go 1.16

require (
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/lytics/retry v1.2.0
	github.com/spf13/pflag v1.0.5 // indirect
	go.etcd.io/etcd v0.0.0-20190917205325-a14579fbfb1a
	go.etcd.io/etcd/client/v3 v3.5.0 // indirect
	go.etcd.io/etcd/server/v3 v3.5.0 // indirect
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4
	google.golang.org/grpc v1.38.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
)

retract (
	v3.2.4 // contains a very bad bug
)
