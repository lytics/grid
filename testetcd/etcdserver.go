package testetcd

import (
	"testing"

	"github.com/coreos/etcd/clientv3"
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
