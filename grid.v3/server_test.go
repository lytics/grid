package grid

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lytics/grid/grid.v3/testetcd"
)

type startStopActor struct {
	started chan bool
	stopped chan bool
}

func (a *startStopActor) Act(c context.Context) {
	a.started <- true
	<-c.Done()
	a.stopped <- true
}

func TestServerStartStop(t *testing.T) {
	const (
		timeout = 20 * time.Second
	)

	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	a := &startStopActor{
		started: make(chan bool),
		stopped: make(chan bool),
	}

	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterDef("leader", func(_ []byte) Actor { return a })

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		defer close(done)
		err := server.Serve(lis)
		if err != nil {
			done <- err
		}
	}()

	for {
		select {
		case <-time.After(timeout):
			t.Fatal("timeout")
		case <-a.started:
			server.Stop()
		case <-a.stopped:
			select {
			case <-time.After(timeout):
				t.Fatal("timeout")
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
				return
			}
		}
	}
}

func TestServerStartNoEtcdRunning(t *testing.T) {
	const (
		timeout = 20 * time.Second
	)

	// Start etcd, but shut it down right away.
	etcd, cleanup := testetcd.StartAndConnect(t)
	cleanup()

	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	err = server.Serve(lis)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestServerStartThenEtcdStop(t *testing.T) {
	t.Skip()

	a := &startStopActor{
		started: make(chan bool),
		stopped: make(chan bool),
	}

	etcd, cleanup := testetcd.StartAndConnect(t)

	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"})
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterDef("leader", func(_ []byte) Actor { return a })

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		defer close(done)
		err := server.Serve(lis)
		if err != nil {
			done <- err
		}
	}()

	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-a.started:
			err := cleanup()
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(90 * time.Second):
			t.Fatal("timeout")
		}
	}
}
