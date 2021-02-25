package testetcd

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
	embedv3 "go.etcd.io/etcd/server/v3/embed"
)

type Embedded struct {
	Cfg    *embedv3.Config
	Etcd   *embedv3.Etcd
	tmpDir string
}

func NewEmbedded() *Embedded {
	ecfg := embedv3.NewConfig()
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

	e, err := embedv3.StartEtcd(ecfg)
	if err != nil {
		panic(fmt.Sprintf("failed to start embedded etcd: %v", err))
	}

	select {
	case <-e.Server.ReadyNotify():
		fmt.Printf("Started embedded etcd on url: %v", clientURL)

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

func StartAndConnect(t testing.TB, endpoints []string) *etcdv3.Client {
	cfg := etcdv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
	}
	etcd, err := etcdv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return etcd
}
