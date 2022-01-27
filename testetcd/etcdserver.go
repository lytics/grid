package testetcd

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type Embedded struct {
	Cfg    *embed.Config
	Etcd   *embed.Etcd
	tmpDir string
}

func NewEmbedded() *Embedded {
	ecfg := embed.NewConfig()
	ecfg.Logger = "zap"
	ecfg.LogLevel = "error"
	tmpDir, err := ioutil.TempDir("", "etcd_")
	if err != nil {
		panic(fmt.Sprintf("error making temp dir for embedded etcd: %v", err))
	}
	ecfg.Dir = tmpDir

	clientPort := 1024 + rand.Intn(30000)
	peerPort := 1024 + rand.Intn(30000)

	ecfg.InitialCluster = fmt.Sprintf("default=http://localhost:%v", peerPort)

	peerURL, _ := url.Parse(fmt.Sprintf("http://localhost:%v", peerPort))
	ecfg.APUrls = []url.URL{*peerURL}
	ecfg.LPUrls = []url.URL{*peerURL}

	clientURL, _ := url.Parse(fmt.Sprintf("http://localhost:%v", clientPort))
	ecfg.ACUrls = []url.URL{*clientURL}
	ecfg.LCUrls = []url.URL{*clientURL}

	e, err := embed.StartEtcd(ecfg)
	if err != nil {
		panic(fmt.Sprintf("failed to start embedded etcd: %v", err))
	}

	select {
	case <-e.Server.ReadyNotify():
		fmt.Printf("Started embedded etcd on url: %v\n", clientURL)

	case <-time.After(10 * time.Second):
		e.Server.Stop() // trigger a shutdown
		panic("Embedded etcd server took too long to start!")
	}

	return &Embedded{
		Cfg:    ecfg,
		Etcd:   e,
		tmpDir: tmpDir,
	}
}

func (e *Embedded) Close() {
	os.RemoveAll(e.tmpDir)
	e.Etcd.Close()
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
