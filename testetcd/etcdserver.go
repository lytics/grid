package testetcd

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type Embedded struct {
	Cfg  *embed.Config
	Etcd *embed.Etcd
}

func NewEmbedded(t testing.TB) *Embedded {
	ecfg := embed.NewConfig()
	ecfg.Logger = "zap"
	ecfg.LogLevel = "error"
	ecfg.Dir = t.TempDir()

	clientPort := 1024 + rand.Intn(30000)
	peerPort := 1024 + rand.Intn(30000)

	ecfg.InitialCluster = fmt.Sprintf("default=http://localhost:%v", peerPort)

	peerURL, err := url.Parse(fmt.Sprintf("http://localhost:%v", peerPort))
	if err != nil {
		t.Fatalf("parsing peerURL: %v", err)
	}
	ecfg.APUrls = []url.URL{*peerURL}
	ecfg.LPUrls = []url.URL{*peerURL}

	clientURL, err := url.Parse(fmt.Sprintf("http://localhost:%v", clientPort))
	if err != nil {
		t.Fatalf("creating clientURL: %v", err)
	}
	ecfg.ACUrls = []url.URL{*clientURL}
	ecfg.LCUrls = []url.URL{*clientURL}

	e, err := embed.StartEtcd(ecfg)
	if err != nil {
		t.Fatalf("starting embedded etcd: %v", err)
	}
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
		t.Logf("Started embedded etcd on url: %v\n", clientURL)
	case <-time.After(10 * time.Second):
		t.Fatal("Embedded etcd server took too long to start!")
	}

	return &Embedded{
		Cfg:  ecfg,
		Etcd: e,
	}
}

func (e *Embedded) Endpoints() []string {
	ep := make([]string, len(e.Cfg.ACUrls))
	for i, u := range e.Cfg.ACUrls {
		ep[i] = u.String()
	}
	return ep
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
