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

func StartAndConnect(t testing.TB) *clientv3.Client {
	t.Helper()

	endpoints := os.Getenv("GRID_ETCD_ENDPOINTS")
	if endpoints == "" {
		t.Log("GRID_ETCD_ENDPOINTS is not set")
		endpoints = "http://127.0.0.1:2379"
	}

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := etcd.Close(); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	return etcd
}
