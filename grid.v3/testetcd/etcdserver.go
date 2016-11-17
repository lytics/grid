package testetcd

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"
)

type Cleanupfn func() error

func StartEtcd(t *testing.T) (*embed.Config, Cleanupfn, error) {
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
		return nil, nil, fmt.Errorf("failed to create the etcd log: err:%v", err)
	}
	//comment out to get etcd logs on stderr
	capnslog.SetFormatter(capnslog.NewPrettyFormatter(f, cfg.Debug))

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed starting server: err:%v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		return nil, nil, fmt.Errorf("Server took too long to start!")
	}

	select {
	case err := <-e.Err():
		return nil, nil, fmt.Errorf("server return an error during startup: err:%v", err)
	default:
	}

	return cfg, func() error {
		e.Server.Stop() // trigger a shutdown
		select {
		case err := <-e.Err():
			return fmt.Errorf("server stop returned an error:%v", err)
		default:
		}

		e.Close()
		select {
		case err := <-e.Err():
			return fmt.Errorf("closing server return an error:%v", err)
		default:
		}
		return os.Remove(cfg.Dir)
	}, nil
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
