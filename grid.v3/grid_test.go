package grid

import (
	"context"
	"net"
	"testing"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/testetcd"
)

func TestServerExample(t *testing.T) {
	etcd, cleanup := bootstrap(t)
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

	peers, err := client.Peers(time.Second)
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

func bootstrap(t *testing.T) (*etcdv3.Client, testetcd.Cleanupfn) {
	srvcfg, cleanup, err := testetcd.StartEtcd(t)
	if err != nil {
		t.Fatalf("err:%v", err)
	}

	endpoints := []string{}
	for _, u := range srvcfg.LCUrls {
		endpoints = append(endpoints, u.String())
	}

	cfg := etcdv3.Config{
		Endpoints: endpoints,
	}

	etcd, err := etcdv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return etcd, cleanup
}
