package grid

import (
	"net"
	"testing"
	"time"

	"github.com/lytics/grid/grid.v3/testetcd"
)

func TestNewClient(t *testing.T) {
	etcd, cleanup := testetcd.StartEtcd(t)
	defer cleanup()

	client, err := NewClient(etcd, ClientCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

func TestNewClientWithNilEtcd(t *testing.T) {
	_, err := NewClient(nil, ClientCfg{Namespace: "testing"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientClose(t *testing.T) {
	etcd, cleanup := testetcd.StartEtcd(t)
	defer cleanup()

	client, err := NewClient(etcd, ClientCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}

	// The type clientAndConn checks if it is nil
	// in its close method, and returns an error.
	client.clientsAndConns["mock"] = nil
	err = client.Close()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientRequestWithBadReceiverName(t *testing.T) {
	etcd, cleanup := testetcd.StartEtcd(t)
	defer cleanup()

	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		err = server.Serve(lis)
		if err != nil {
			panic(err.Error())
		}
	}()
	time.Sleep(2 * time.Second)

	client, err := NewClient(etcd, ClientCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	res, err := client.Request(2*time.Second, "mock", NewActorDef("mock"))
	if err == nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal(res)
	}
}
