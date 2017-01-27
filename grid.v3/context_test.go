package grid

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lytics/grid/grid.v3/testetcd"
)

type contextActor struct {
	started chan bool
	ctx     context.Context
}

func (a *contextActor) Act(c context.Context) {
	a.ctx = c
	a.started <- true
}

func TestContextError(t *testing.T) {
	// Create a context that is not valid to use
	// with the grid context methods. The context
	// is not valid because it does not contain
	// all the needed keys and values.
	c := context.Background()

	id, err := ContextActorID(c)
	if err == nil {
		t.Fatal("expected error")
	}
	if id != "" {
		t.Fatal("expected zero value")
	}

	name, err := ContextActorName(c)
	if err == nil {
		t.Fatal("expected error")
	}
	if name != "" {
		t.Fatal("expected zero value")
	}

	namespace, err := ContextActorNamespace(c)
	if err == nil {
		t.Fatal("expected error")
	}
	if namespace != "" {
		t.Fatal("expected zero value")
	}

	server, err := ContextActorServer(c)
	if err == nil {
		t.Fatal("expected error")
	}
	if server != nil {
		t.Fatal("expected zero value")
	}
}

func TestValidContext(t *testing.T) {
	const timeout = 2 * time.Second

	etcd, cleanup := testetcd.StartAndConnect(t)
	defer cleanup()

	a := &contextActor{started: make(chan bool)}

	g := func(def *ActorDef) (Actor, error) {
		if def.Type == "leader" {
			return a, nil
		}
		return nil, nil
	}

	server, err := NewServer(etcd, ServerCfg{Namespace: "testing"}, MakerFunc(g))
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
	time.Sleep(timeout)

	for {
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("timeout")
		case <-a.started:
			server.Stop()

			id, err := ContextActorID(a.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if id == "" {
				t.Fatal("expected non-zero value")
			}

			name, err := ContextActorName(a.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if name == "" {
				t.Fatal("expected non-zero value")
			}

			namespace, err := ContextActorNamespace(a.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if namespace == "" {
				t.Fatal("expected non-zero value")
			}

			server, err := ContextActorServer(a.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if server == nil {
				t.Fatal("expected non-zero value")
			}
			return
		}
	}
}
