package grid

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lytics/grid/v3/testetcd"
	"github.com/lytics/retry"
)

func TestQuery(t *testing.T) {
	const (
		nrPeers = 2
		backoff = 10 * time.Second
		timeout = 1 * time.Second
	)

	namespace := newNamespace()

	embed := testetcd.NewEmbedded(t)
	etcd := testetcd.StartAndConnect(t, embed.Endpoints())

	client, err := NewClient(etcd, ClientCfg{Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("closing client: %v", err)
		}
	})

	for i := 1; i <= nrPeers; i++ {
		s, err := NewServer(etcd, ServerCfg{Namespace: namespace})
		if err != nil {
			t.Fatal(err)
		}

		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := s.Serve(lis); err != nil {
				t.Logf("serving: %v", err)
			}
		}()
		t.Cleanup(func() { <-done })
		t.Cleanup(s.Stop)

		// Check for server as a peer.
		var peers []*QueryEvent
		retry.X(6, backoff, func() bool {
			peers, err = client.Query(timeout, Peers)
			t.Logf("peers: %v", peers)
			return err != nil || len(peers) != i
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(peers) != i {
			t.Fatalf("expected number of peers: %v, found: %v", i, len(peers))
		}
	}
}

func TestQueryWatch(t *testing.T) {
	const (
		nrPeers = 2
		backoff = 10 * time.Second
		timeout = 1 * time.Second
	)

	namespace := newNamespace()
	embed := testetcd.NewEmbedded(t)
	etcd := testetcd.StartAndConnect(t, embed.Endpoints())

	client, err := NewClient(etcd, ClientCfg{Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("closing client: %v", err)
		}
	})

	initialPeers, watch, err := client.QueryWatch(context.Background(), Peers)
	if err != nil {
		t.Fatal(err)
	}
	if len(initialPeers) != 0 {
		t.Fatal("expected 0 peers")
	}

	for i := 1; i <= nrPeers; i++ {
		s, err := NewServer(etcd, ServerCfg{Namespace: namespace})
		if err != nil {
			t.Fatal(err)
		}

		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := s.Serve(lis); err != nil {
				t.Logf("error serving: %v", err)
			}
		}()
		t.Cleanup(func() { <-done })
		t.Cleanup(s.Stop)
	}

	// Monitor the watch channel to confirm that started
	// servers are eventually found.
	found := make(map[string]bool)
	for {
		select {
		case <-time.After(10 * time.Second):
			t.Fatalf("expected number of peers: %v, found: %v", nrPeers, len(found))
		case e := <-watch:
			if e.eventType == EntityFound {
				found[e.Name()] = true
				t.Logf("found peer: %v", e.Name())
			}
			if len(found) == 2 {
				return
			}
		}
	}
}
