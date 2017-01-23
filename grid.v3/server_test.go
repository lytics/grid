package grid

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lytics/grid/grid.v3/testetcd"
)

func TestServerExample(t *testing.T) {
	etcd, cleanup := testetcd.StartEtcd(t)
	defer cleanup()

	grid := WorkerGrid(make(chan bool, 1))

	server, err := NewServer(etcd, ServerCfg{Namespace: "example_grid"}, grid)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	lis, err := net.Listen("tcp", "localhost:0") // Let the OS pick a port for us.
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	go func() {
		err = server.Serve(lis)
		if err != nil {
			panic(err.Error())
		}
	}()

	time.Sleep(1 * time.Second)

	client, err := NewClient(etcd, ClientCfg{Namespace: "example_grid"})
	if err != nil {
		t.Fatal(err)
	}

	peers, err := client.Query(time.Second, Peers)
	if err != nil {
		t.Fatal(err)
	}

	if len(peers) != 1 {
		time.Sleep(30 * time.Second)
		t.Fatal(peers)
	}

	_, err = client.Request(time.Second, peers[0], NewActorDef("worker"))
	if err != nil {
		time.Sleep(30 * time.Second)
		t.Fatal(err, peers[0])
	}

	time.Sleep(1 * time.Second)

	// Stop will wait.
	server.Stop()

	select {
	case <-grid:
	default:
		t.Fatal("worker did not start and stop")
	}
}

type WorkerGrid chan bool

func (g WorkerGrid) MakeActor(def *ActorDef) (Actor, error) {
	switch def.Type {
	case "worker":
		return &ExampleWorker{finished: g}, nil
	}
	return nil, nil
}

type ExampleWorker struct {
	finished WorkerGrid
}

func (a *ExampleWorker) Act(c context.Context) {
	<-c.Done()
	a.finished <- true
}