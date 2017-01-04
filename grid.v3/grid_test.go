package grid

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/testetcd"
)

func TestServerExample(t *testing.T) {
	etcd, cleanup := bootstrap(t)
	defer cleanup()

	grid := &WorkerGrid{
		WorkerStopped: make(chan bool, 1),
	}

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
		t.Fatal(peers)
	}

	_, err = client.Request(time.Second, peers[0], NewActorDef("worker"))
	if err != nil {
		t.Fatal(err, peers[0])
	}

	time.Sleep(1 * time.Second)

	// Stop will wait.
	server.Stop()

	select {
	case <-grid.WorkerStopped:
	default:
		t.Fatal("worker did not start and stop")
	}
}

type WorkerGrid struct {
	WorkerStopped chan bool
}

func (*WorkerGrid) MakeActor(def *ActorDef) (Actor, error) {
	switch def.Type {
	case "worker":
		return &ExampleWorker{}, nil
	}
	return nil, fmt.Errorf("unknow actor type: %v", def.Type)
}

type ExampleWorker struct {
	state *WorkerGrid
}

func (a *ExampleWorker) Act(c context.Context) {
	<-c.Done()
	a.state.WorkerStopped <- true
}

func bootstrap(t *testing.T) (*etcdv3.Client, testetcd.Cleanupfn) {
	srvcfg, cleanup, err := testetcd.StartEtcd(t)
	if err != nil {
		t.Fatalf("err:%v", err)
	}

	endpoints := []string{}
	for _, u := range srvcfg.LCendpoints {
		urls = append(urls, u.String())
	}

	cfg := etcdv3.Config{
		Endpoints: urls,
	}

	etcd, err := etcdv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return etcd, cleanup
}
