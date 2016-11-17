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
	client, cleanup := bootstrap(t)
	defer cleanup()

	g, err := NewServer(client, "example_grid", ExampleGrid{})
	abort := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	go func() {
		<-abort  //shutting down...
		g.Stop() //shutdown complete
		close(stopped)
	}()

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	// After calling Serve one node within your Grid will be elected as
	// a "leader" and that grid's MakeActor() will be called with a
	// def.Type == "leader".  You can think of this as the main for the
	// grid cluster.
	err = g.Serve(lis)
	if err != nil {
		t.Fatalf("listen serve: %v", err)
	}

	time.Sleep(2 * time.Second)
	close(abort)
	<-stopped
}

type ExampleGrid struct{}

func (e ExampleGrid) MakeActor(def *ActorDef) (Actor, error) {
	switch def.Type {
	case "leader":
		return &ExampleLeader{}, nil
	}
	return nil, fmt.Errorf("unknow actor type: %v", def.Type)
}

type ExampleLeader struct{}

func (a *ExampleLeader) Act(c context.Context) {
	fmt.Println("Im the leader...")
}

func bootstrap(t *testing.T) (*etcdv3.Client, testetcd.Cleanupfn) {
	srvcfg, cleanup, err := testetcd.StartEtcd(t)
	if err != nil {
		t.Fatalf("err:%v", err)
	}

	urls := []string{}
	for _, u := range srvcfg.LCUrls {
		urls = append(urls, u.String())
	}
	cfg := etcdv3.Config{
		Endpoints: urls,
	}
	client, err := etcdv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return client, cleanup
}
