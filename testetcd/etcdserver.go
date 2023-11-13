package testetcd

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Embedded struct {
	endpoints []string
}

func NewEmbedded(t testing.TB) *Embedded {
	endpointsStr := os.Getenv("GRID_ETCD_ENDPOINTS")
	if endpointsStr == "" {
		t.Log("GRID_ETCD_ENDPOINTS is not set")
		endpointsStr = "http://127.0.0.1:2379"
	}
	endpoints := strings.Split(endpointsStr, ",")
	return &Embedded{
		endpoints: endpoints,
	}
}

func (e *Embedded) Endpoints() []string {
	return e.endpoints
}

func StartAndConnect(t testing.TB, endpoints []string) *clientv3.Client {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
	}
	etcd, err := clientv3.New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := etcd.Close(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	return etcd
}
