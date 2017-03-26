package grid

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/testetcd"
)

type busyActor struct {
	ready  chan bool
	server *Server
}

func (a *busyActor) Act(c context.Context) {
	name, err := ContextActorName(c)
	if err != nil {
		return
	}

	mailbox, err := NewMailbox(a.server, name, 0)
	if err != nil {
		return
	}
	defer mailbox.Close()

	// Don't bother listening
	// to the mailbox, too
	// busy.
	a.ready <- true
	<-c.Done()
}

type echoActor struct {
	ready  chan bool
	server *Server
}

func (a *echoActor) Act(c context.Context) {
	name, err := ContextActorName(c)
	if err != nil {
		return
	}

	mailbox, err := NewMailbox(a.server, name, 1)
	if err != nil {
		return
	}
	defer mailbox.Close()

	a.ready <- true
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
	client.cs = newClientStats()

	// The type clientAndConn checks if it is nil
	// in its close method, and returns an error.
	client.clientsAndConns["mock"] = nil
	err = client.Close()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientRequestWithUnregisteredMailbox(t *testing.T) {
	const timeout = 2 * time.Second

	// Bootstrap.
	_, cleanup, server, client := bootstrapClientTest(t)
	defer cleanup()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Send a request to some random name.
	res, err := client.Request(timeout, "mock", NewActorDef("mock"))
	if err != ErrUnregisteredMailbox {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal(res)
	}

	if v := client.cs.counters[numErrUnregisteredMailbox]; v == 0 {
		t.Fatal("expected non-zero error count")
	}
}

func TestClientRequestWithUnknownMailbox(t *testing.T) {
	const timeout = 2 * time.Second

	// Bootstrap.
	_, cleanup, server, client := bootstrapClientTest(t)
	defer cleanup()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Place a bogus entry in etcd with
	// a matching name.
	timeoutC, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := server.registry.Register(timeoutC, "testing.mailbox.mock")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	// Send a request to some random name.
	res, err := client.Request(timeout, "mock", NewActorDef("mock"))
	if err == nil {
		t.Fatal("expected error")
	}
	if res != nil {
		t.Fatal(res)
	}
	if !strings.Contains(err.Error(), ErrUnknownMailbox.Error()) {
		t.Fatal(err)
	}

	if v := client.cs.counters[numErrUnknownMailbox]; v == 0 {
		t.Fatal("expected non-zero error count")
	}
}

func TestClientWithRunningReceiver(t *testing.T) {
	const (
		timeout  = 2 * time.Second
		expected = "testing 1, 2, 3"
	)

	// Bootstrap.
	_, cleanup, server, client := bootstrapClientTest(t)
	defer cleanup()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Create echo actor.
	a := &echoActor{ready: make(chan bool)}

	// Create the grid.
	g := func(def *ActorDef) (Actor, error) {
		if def.Type == "echo" {
			return a, nil
		}
		return nil, nil
	}

	// Set grid definition.
	server.SetDefinition(FromFunc(g))

	// Set server on echo actor.
	a.server = server

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
	<-a.ready

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

	if v := client.cs.counters[numErrConnectionUnavailable]; v != 0 {
		t.Fatal("expected zero error count")
	}
}

func TestClientWithErrConnectionIsUnavailable(t *testing.T) {
	const (
		timeout  = 2 * time.Second
		expected = "hello"
	)

	// Bootstrap.
	_, cleanup, server, client := bootstrapClientTest(t)
	defer cleanup()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Create echo actor.
	a := &echoActor{ready: make(chan bool)}

	// Create the grid.
	g := func(def *ActorDef) (Actor, error) {
		if def.Type == "echo" {
			return a, nil
		}
		return nil, nil
	}

	// Set grid definition.
	server.SetDefinition(FromFunc(g))

	// Set server on echo actor.
	a.server = server

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
	<-a.ready

	// Make a request to echo actor.
	res, err = client.Request(timeout, "echo", expected)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response")
	}

	// Stop the server.
	server.Stop()

	// Wait for the gRPC to be really stopped.
	time.Sleep(timeout)

	// Make the request again.
	res, err = client.Request(timeout, "echo", expected)
	if err == nil {
		t.Fatal("expected error")
	}
	if res != nil {
		t.Fatal(res)
	}
	if !strings.Contains(err.Error(), "the connection is unavailable") {
		t.Fatal(err)
	}

	if v := client.cs.counters[numErrConnectionUnavailable]; v == 0 {
		t.Fatal("expected non-zero error count")
	}
}

func TestClientWithBusyReceiver(t *testing.T) {
	const (
		timeout  = 2 * time.Second
		expected = "testing 1, 2, 3"
	)

	// Bootstrap.
	_, cleanup, server, client := bootstrapClientTest(t)
	defer cleanup()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Create busy actor.
	a := &busyActor{ready: make(chan bool)}

	// Create the grid.
	g := func(def *ActorDef) (Actor, error) {
		if def.Type == "busy" {
			return a, nil
		}
		return nil, nil
	}
	server.SetDefinition(FromFunc(g))

	// Set server on busy actor.
	a.server = server

	// Discover some peers.
	peers, err := client.Query(timeout, Peers)
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 {
		t.Fatal("expected 1 peer")
	}

	// Start the busy actor on the first peer.
	res, err := client.Request(timeout, peers[0].Name(), NewActorDef("busy"))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected response")
	}

	// Wait for busy actor to start.
	<-a.ready

	// Make a request to busy actor.
	res, err = client.Request(timeout, "busy", expected)
	if err == nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal("expected response")
	}
	if !strings.Contains(err.Error(), ErrReceiverBusy.Error()) {
		t.Fatal(err)
	}
}

func TestClientStats(t *testing.T) {
	cs := newClientStats()
	cs.Inc(numGetWireClient)
	cs.Inc(numDeleteAddress)
	if cs.counters[numGetWireClient] != 1 {
		t.Fatal("expected count of 1")
	}
	if cs.counters[numDeleteAddress] != 1 {
		t.Fatal("expected count of 1")
	}
	switch cs.String() {
	case "numGetWireClient:1, numDeleteAddress:1":
	case "numDeleteAddress:1, numGetWireClient:1":
	default:
		t.Fatal("expected string: 'numGetWireClient:1'")
	}
}

func TestNilClientStats(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("expected no panic")
		}
	}()
	var cs *clientStats
	cs.Inc(numGetWireClient)
}

func bootstrapClientTest(t *testing.T) (*clientv3.Client, testetcd.Cleanup, *Server, *Client) {
	// Start etcd.
	etcd, cleanup := testetcd.StartAndConnect(t)

	// Create the server.
	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}

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
	time.Sleep(2 * time.Second)

	// Create a grid client.
	client, err := NewClient(etcd, ClientCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}

	return etcd, cleanup, server, client
}
