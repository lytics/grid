package grid

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lytics/grid/grid.v3/testetcd"
)

type echoActor chan bool

func (a echoActor) Act(c context.Context) {
	name, err := ContextActorName(c)
	if err != nil {
		return
	}

	server, err := ContextActorServer(c)
	if err != nil {
		return
	}

	mailbox, err := NewMailbox(server, name, 1)
	if err != nil {
		return
	}

	a <- true
	for {
		select {
		case <-c.Done():
			return
		case req := <-mailbox.C:
			req.Respond(req.Msg())
		}
	}
}

func TestNewClient(t *testing.T) {
	etcd, cleanup := testetcd.StartAndConnect(t)
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
	// Start etcd.
	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	// Create client.
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
	const timeout = 2 * time.Second

	// Start etcd.
	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	// Create grid server.
	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}

	// Create listener.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	// Start server in background.
	done := make(chan error, 1)
	go func() {
		err = server.Serve(lis)
		if err != nil {
			done <- err
		}
	}()
	time.Sleep(timeout)

	// Create client.
	client, err := NewClient(etcd, ClientCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Send a request to some random name.
	res, err := client.Request(timeout, "mock", NewActorDef("mock"))
	if err == nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal(res)
	}

	// Stop the server.
	server.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	default:
	}
}

func TestClientWithRunningReceiver(t *testing.T) {
	const (
		timeout  = 2 * time.Second
		expected = "testing 1, 2, 3"
	)

	// Start etcd.
	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	// Create echo actor.
	a := make(echoActor)

	// Create the grid.
	g := func(def *ActorDef) (Actor, error) {
		if def.Type == "echo" {
			return a, nil
		}
		return nil, nil
	}

	// Create the server.
	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	server.SetDefinition(FromFunc(g))

	// Create the listener on a random port.
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	// Start the server in the background.
	done := make(chan error, 1)
	go func() {
		err = server.Serve(lis)
		if err != nil {
			done <- err
		}
	}()
	time.Sleep(timeout)

	// Create a grid client.
	client, err := NewClient(etcd, ClientCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Discover some peers.
	peers, err := client.Query(timeout, Peers)
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 {
		t.Fatal("expected 1 peer")
	}

	// Start the echo actor on the first peer.
	res, err := client.Request(timeout, peers[0].Name(), NewActorDef("echo"))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response")
	}

	// Wait for echo actor to start.
	<-a

	// Make a request to echo actor.
	res, err = client.Request(timeout, "echo", expected)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response")
	}

	// Expect the same string back as a response.
	switch res.(type) {
	case string:
		if res != expected {
			t.Fatal("expected: %v, received: %v", expected, res)
		}
	default:
		t.Fatal("expected type: string, received type: %T", res)
	}
}
