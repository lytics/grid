package testetcd

import (
	"testing"

	"go.etcd.io/etcd/clientv3"
)

func StartAndConnect(t testing.TB) *clientv3.Client {
	cfg := clientv3.Config{
		Endpoints: []string{"localhost:2379"},
	}
	etcd, err := clientv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return etcd
}
