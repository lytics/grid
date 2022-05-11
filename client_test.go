package grid

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lytics/grid/v3/testetcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type busyActor struct {
	ready  chan bool
	server *Server
}

var etcdEndpoints []string

func TestMain(m *testing.M) {
	embed := testetcd.NewEmbedded()
	defer embed.Etcd.Close()
	etcdEndpoints = []string{embed.Cfg.ACUrls[0].String()}
	r := m.Run()
	os.Exit(r)
}

func (a *busyActor) Act(c context.Context) {
	name, err := ContextActorName(c)
	if err != nil {
		return
	}

	mailbox, err := a.server.NewMailbox(name, 0)
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

	mailbox, err := a.server.NewMailbox(name, 1)
	if err != nil {
		return
	}
	defer mailbox.Close()

	a.ready <- true
	for {
		select {
		case <-c.Done():
			return
		case req, ok := <-mailbox.C():
			if !ok {
				return
			}
			req.Respond(req.Msg())
		}
	}
}

func init() {
	Register(EchoMsg{})
}

func TestNewClient(t *testing.T) {
	etcd := testetcd.StartAndConnect(t, etcdEndpoints)

	client, err := NewClient(etcd, ClientCfg{Namespace: newNamespace()})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

func TestNewClientWithNilEtcd(t *testing.T) {
	_, err := NewClient(nil, ClientCfg{Namespace: newNamespace()})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestClientClose(t *testing.T) {
	// Start etcd.
	etcd := testetcd.StartAndConnect(t, etcdEndpoints)

	// Create client.
	client, err := NewClient(etcd, ClientCfg{Namespace: newNamespace()})
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
	const timeout = 3 * time.Second

	// Bootstrap.
	etcd, server, client := bootstrapClientTest(t)
	defer etcd.Close()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Send a request to some random name.
	res, err := client.Request(timeout, "mock", NewActorStart("mock"))
	if err != ErrUnregisteredMailbox {
		t.Fatal(err)
	}
	if res != nil {
		t.Logf("expected res to be nil")
		t.Fatal(res)
	}

	if v := client.cs.counters[numErrUnregisteredMailbox]; v == 0 {
		t.Fatal("expected non-zero error count")
	}
}

func TestClientRequestWithUnknownMailbox(t *testing.T) {
	const timeout = 3 * time.Second

	// Bootstrap.
	etcd, server, client := bootstrapClientTest(t)
	defer etcd.Close()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Place a bogus entry in etcd with
	// a matching name.
	timeoutC, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := server.registry.Register(timeoutC, client.cfg.Namespace+".mailbox.mock")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	// Send a request to some random name.
	res, err := client.Request(timeout, "mock", NewActorStart("mock"))
	if err == nil {
		t.Fatal("expected error")
	}
	if res != nil {
		t.Fatal(res)
	}
	if !strings.Contains(err.Error(), ErrUnknownMailbox.Error()) {
		t.Logf("error compare failed: str:%v substr:%v", err.Error(), ErrUnknownMailbox.Error())
		t.Fatal(err)
	}

	if v := client.cs.counters[numErrUnknownMailbox]; v == 0 {
		t.Fatal("expected non-zero error count")
	}
}

func TestClientBroadcast(t *testing.T) {
	const timeout = 3 * time.Second
	etcd, server, client := bootstrapClientTest(t)
	defer etcd.Close()
	defer server.Stop()
	defer client.Close()

	a := &echoActor{ready: make(chan bool), server: server}
	server.RegisterDef("echo", func([]byte) (Actor, error) { return a, nil })

	peers, err := client.Query(timeout, Peers)
	if err != nil {
		t.Fatalf("failed to query peers: %v", err)
	} else if len(peers) != 1 {
		t.Fatal("expected 1 peer")
	}
	peer := peers[0].Name()

	const echoType = "echo"
	startEchoActor := func(name string) {
		actor := NewActorStart(name)
		actor.Type = echoType
		res, err := client.Request(timeout, peer, actor)
		if err != nil {
			t.Fatalf("failed to start echo actor: %v", err)
		} else if res == nil {
			t.Fatal("expected a response")
		}
		<-a.ready
	}

	// start up the actors
	const numActors = 2
	for i := 0; i < numActors; i++ {
		startEchoActor(fmt.Sprintf("echo-%d", i))
	}

	msg := &EchoMsg{Msg: "lol"}
	t.Run("broadcast-all", func(t *testing.T) {
		g := NewListGroup("echo-0", "echo-1")
		res, err := client.Broadcast(timeout, g, msg)
		if err != nil {
			t.Fatalf("failed to broadcast message: %v", err)
		} else if len(res) != numActors {
			t.Fatal("expected response")
		}
		for i := 0; i < numActors; i++ {
			actor := fmt.Sprintf("echo-%d", i)
			r, ok := res[actor]
			if !ok {
				t.Fatalf("could not find result for actor %s", actor)
			} else if r.Err != nil {
				t.Fatalf("unexpected error in result for actor %s: %v", actor, r.Err)
			} else if r.Val == nil {
				t.Fatalf("expected value in result for actor %s", actor)
			}
		}
	})

	t.Run("broadcast-fastest", func(t *testing.T) {
		g := NewListGroup("echo-0", "echo-1")
		res, err := client.Broadcast(timeout, g.Fastest(), msg)
		if err != nil {
			t.Fatalf("failed to broadcast message: %v", err)
		} else if len(res) != numActors {
			t.Fatal("unexpected response")
		}
		numCancelled := 0
		numSuccess := 0
		for _, r := range res {
			if r.Err == nil && r.Val != nil {
				numSuccess++
			}
			if r.Err != nil && strings.Contains(r.Err.Error(), "context canceled") {
				numCancelled++
			}
		}

		if numSuccess < 1 {
			t.Fatalf("expected 1 or more successes, got %d", numSuccess)
		}
		if numCancelled > numSuccess {
			t.Fatalf("expected more successes than cancellations, got %d cancelled, %d successes", numCancelled, numSuccess)
		}
	})

	t.Run("broadcast-retry", func(t *testing.T) {
		// while echo-0 and echo-1 are registed/running, echo-2 is not
		g := NewListGroup("echo-0", "echo-1", "echo-2")

		resultSet := make(BroadcastResult)
		tmpSet, err := client.Broadcast(timeout, g.ExceptSuccesses(resultSet), msg)
		if err != ErrIncompleteBroadcast {
			t.Fatal("expected a broadcast-error")
		} else if tmpSet["echo-2"].Err != ErrUnregisteredMailbox {
			t.Fatal("expected unregistered mailbox error")
		}
		resultSet.Add(tmpSet)

		// start the missing actor
		startEchoActor("echo-2")

		tmpSet, err = client.Broadcast(timeout, g.ExceptSuccesses(resultSet), msg)
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		resultSet.Add(tmpSet)

		// ensure that the resultSet contains results for all 3 echo-actors
		if sz := len(resultSet); sz != 3 {
			t.Fatalf("expected 2 results in broadcast-result, got %d", sz)
		}
		for actor, v := range resultSet {
			if v.Err != nil {
				t.Fatalf("expected nil error for actor: %s, result: %+v", actor, v)
			}
		}
	})
}

func TestClientWithRunningReceiver(t *testing.T) {
	const timeout = 3 * time.Second
	expected := &EchoMsg{Msg: "testing 1, 2, 3"}

	// Bootstrap.
	etcd, server, client := bootstrapClientTest(t)
	defer etcd.Close()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Create echo actor.
	a := &echoActor{ready: make(chan bool)}

	// Set grid definition.
	server.RegisterDef("echo", func(_ []byte) (Actor, error) { return a, nil })

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
	res, err := client.Request(timeout, peers[0].Name(), NewActorStart("echo"))
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
	switch res := res.(type) {
	case *EchoMsg:
		if res.Msg != expected.Msg {
			t.Fatalf("expected: %v, received: %v", expected, res)
		}
	default:
		t.Fatalf("expected type: string, received type: %T", res)
	}

	if v := client.cs.counters[numErrConnectionUnavailable]; v != 0 {
		t.Fatal("expected zero error count")
	}
}

func TestClientWithErrConnectionIsUnregistered(t *testing.T) {
	const timeout = 3 * time.Second
	expected := &EchoMsg{Msg: "testing 1, 2, 3"}

	// Bootstrap.
	etcd, server, client := bootstrapClientTest(t)
	defer etcd.Close()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Create echo actor.
	a := &echoActor{ready: make(chan bool)}

	// Set grid definition.
	server.RegisterDef("echo", func(_ []byte) (Actor, error) { return a, nil })

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
	res, err := client.Request(timeout, peers[0].Name(), NewActorStart("echo"))
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
	if !strings.Contains(err.Error(), "unregistered mailbox") {
		t.Fatal(err)
	}

	if v := client.cs.counters[numErrUnregisteredMailbox]; v == 0 {
		t.Fatal("expected non-zero error count")
	}
}

func TestClientWithBusyReceiver(t *testing.T) {
	const timeout = 3 * time.Second
	expected := &EchoMsg{Msg: "testing 1, 2, 3"}

	// Bootstrap.
	etcd, server, client := bootstrapClientTest(t)
	defer etcd.Close()
	defer server.Stop()
	defer client.Close()

	// Set client stats.
	client.cs = newClientStats()

	// Create busy actor.
	a := &busyActor{ready: make(chan bool)}

	server.RegisterDef("busy", func(_ []byte) (Actor, error) { return a, nil })

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
	res, err := client.Request(timeout, peers[0].Name(), NewActorStart("busy"))
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

func bootstrapClientTest(t *testing.T) (*clientv3.Client, *Server, *Client) {
	// Namespace for test.
	namespace := newNamespace()

	// Start etcd.
	etcd := testetcd.StartAndConnect(t, etcdEndpoints)

	// Logger for actors.
	logger := log.New(os.Stderr, namespace+": ", log.LstdFlags)

	// Create the server.
	server, err := NewServer(etcd, ServerCfg{Namespace: namespace, Logger: logger})
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
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		err = server.Serve(lis)
		if err != nil {
			done <- err
		}
	}()
	wg.Wait()
	time.Sleep(3 * time.Second)

	// Create a grid client.
	client, err := NewClient(etcd, ClientCfg{Namespace: namespace, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}

	return etcd, server, client
}
