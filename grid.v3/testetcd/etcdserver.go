package testetcd

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"
)

type Cleanup func() error

func StartAndConnect(t *testing.T) (*clientv3.Client, Cleanup) {
	srvCfg, cleanup := Start(t)

	endpoints := []string{}
	for _, u := range srvCfg.LCUrls {
		endpoints = append(endpoints, u.String())
	}

	cfg := clientv3.Config{
		Endpoints: endpoints,
	}

	etcd, err := clientv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return etcd, cleanup
}

func Start(t *testing.T) (*embed.Config, Cleanup) {
	cfg := embed.NewConfig()
	dir, _ := ioutil.TempDir("", "etcd.testserver.")
	cfg.Dir = dir

	lPUrls, lCUrls, aPUrls, aCUrls := findFreeEtcdUrls()
	cfg.LPUrls = lPUrls
	cfg.LCUrls = lCUrls
	cfg.APUrls = aPUrls
	cfg.ACUrls = aCUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name) //dumb magic that has to be called after updating the URLs.
	cfg.Debug = true

	f, err := os.Create(cfg.Dir + "/" + "etcd.log")
	if err != nil {
		t.Fatal(err)
	}
	//comment out to get etcd logs on stderr
	capnslog.SetFormatter(capnslog.NewPrettyFormatter(f, cfg.Debug))

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(t)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop()
		t.Fatal(err)
	}

	select {
	case err := <-e.Err():
		t.Fatal(err)
	default:
	}

	return cfg, func() error {
		e.Close()
		select {
		case err := <-e.Err():
			if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("closing server returned error: %v", err)
			}
		default:
		}
		return os.RemoveAll(cfg.Dir)
	}
}

func findFreeEtcdUrls() (lPUrls []url.URL, lCUrls []url.URL, aPUrls []url.URL, aCUrls []url.URL) {
	l1, _ := net.Listen("tcp", ":0")
	defer l1.Close()
	p1 := l1.Addr().(*net.TCPAddr).Port

	l2, _ := net.Listen("tcp", ":0")
	defer l2.Close()
	p2 := l2.Addr().(*net.TCPAddr).Port

	lh1 := fmt.Sprintf("http://localhost:%d", p1)
	lh2 := fmt.Sprintf("http://localhost:%d", p2)
	localUrl1, _ := url.Parse(lh1)
	localUrl2, _ := url.Parse(lh2)

	lPUrls = []url.URL{*localUrl1}
	lCUrls = []url.URL{*localUrl2}

	ip := getLocalIP()
	ah1 := fmt.Sprintf("http://%s:%d", ip, p1)
	ah2 := fmt.Sprintf("http://%s:%d", ip, p2)
	adUrl1, _ := url.Parse(ah1)
	adUrl2, _ := url.Parse(ah2)

	aPUrls = []url.URL{*adUrl1}
	aCUrls = []url.URL{*adUrl2}
	return
}

// getLocalIP returns the non loopback local IP of the host
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
