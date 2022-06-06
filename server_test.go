package grid

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lytics/grid/v3/testetcd"
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
	t.Parallel()
	const (
		timeout = 20 * time.Second
	)

	embed := testetcd.NewEmbedded(t)
	etcd := testetcd.StartAndConnect(t, embed.Endpoints())

	a := &startStopActor{
		started: make(chan bool),
		stopped: make(chan bool),
	}

	server, err := NewServer(etcd, ServerCfg{Namespace: newNamespace(t)})
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterDef("leader", func(_ []byte) (Actor, error) { return a, nil })

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
			ctx := server.Context()
			if ctx == nil {
				t.Fatal("expected non-nil context on running server")
			}
			server.Stop()
		case <-a.stopped:
			select {
			case <-time.After(timeout):
				t.Fatal("timeout")
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-server.Context().Done():
				default:
					t.Fatal("expected done context")
				}
				return
			}
		}
	}
}

func TestServerWithFatalError(t *testing.T) {
	t.Parallel()
	const (
		timeout = 20 * time.Second
	)

	embed := testetcd.NewEmbedded(t)
	etcd := testetcd.StartAndConnect(t, embed.Endpoints())

	a := &startStopActor{
		started: make(chan bool),
		stopped: make(chan bool),
	}

	server, err := NewServer(etcd, ServerCfg{Namespace: newNamespace(t)})
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterDef("leader", func(_ []byte) (Actor, error) { return a, nil })

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

	expected := errors.New("testing-fatal-error")
	for {
		select {
		case <-time.After(timeout):
			t.Fatal("timeout")
		case <-a.started:
			server.reportFatalError(expected)
		case <-a.stopped:
			select {
			case <-time.After(timeout):
				t.Fatal("timeout")
			case err := <-done:
				if err != nil && err.Error() == expected.Error() {
					return
				}
				t.Fatal("expected error")
			}
		}
	}
}

func TestServerStartNoEtcdRunning(t *testing.T) {
	t.Parallel()
	// Start etcd, but shut it down right away.
	embed := testetcd.NewEmbedded(t)
	etcd := testetcd.StartAndConnect(t, embed.Endpoints())
	etcd.Close()

	server, err := NewServer(etcd, ServerCfg{Namespace: newNamespace(t)})
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
	t.Parallel()
	t.Skip()

	a := &startStopActor{
		started: make(chan bool),
		stopped: make(chan bool),
	}

	embed := testetcd.NewEmbedded(t)
	etcd := testetcd.StartAndConnect(t, embed.Endpoints())

	server, err := NewServer(etcd, ServerCfg{Namespace: newNamespace(t)})
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterDef("leader", func(_ []byte) (Actor, error) { return a, nil })

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
			err := etcd.Close()
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(90 * time.Second):
			t.Fatal("timeout")
		}
	}
}

// NOTE (2022-06) (mh): WaitUntilStarted() is tested via bootstrapClientTest().
// func TestServerWaitUntilStarted(t *testing.T) {}
