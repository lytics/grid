package grid

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lytics/grid/grid.v3/registry"
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
	client.cs = newClientStats()

	// Send a request to some random name.
	res, err := client.Request(timeout, "mock", NewActorDef("mock"))
	if err != ErrUnregisteredMailbox {
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

	if v := client.cs.counters[numErrUnregisteredMailbox]; v == 0 {
		t.Fatal("expected non-zero error count")
	}
}

func TestClientRequestWithUnknownMailbox(t *testing.T) {
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
	client.cs = newClientStats()

	// Place a bogus entry in etcd with
	// a matching name.
	r, err := registry.New(etcd)
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Start(lis.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	timeoutC, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = r.Register(timeoutC, "testing.mailbox.mock")
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

	// Stop the server.
	server.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	default:
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

	// Start etcd.
	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	// Create echo actor.
	a := &echoActor{ready: make(chan bool)}

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
	defer server.Stop()
	server.SetDefinition(FromFunc(g))

	// Set server on echo actor.
	a.server = server

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
	client.cs = newClientStats()

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

	// Start etcd.
	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	// Create echo actor.
	a := &echoActor{ready: make(chan bool)}

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

	// Set server on echo actor.
	a.server = server

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
	client.cs = newClientStats()

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
	time.Sleep(2 * time.Second)

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

	// Start etcd.
	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	// Create busy actor.
	a := &busyActor{ready: make(chan bool)}

	// Create the grid.
	g := func(def *ActorDef) (Actor, error) {
		if def.Type == "busy" {
			return a, nil
		}
		return nil, nil
	}

	// Create the server.
	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	server.SetDefinition(FromFunc(g))

	// Set server on busy actor.
	a.server = server

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
	client.cs = newClientStats()

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
