package grid

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

func TestPeersWatch(t *testing.T) {
	var cstate = &clusterState{}

	client, cleanup := bootstrap(t)
	defer cleanup()
	errs := &testerrors{}

	abort := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	gridnodes := []*Server{}
	for i := 0; i < 4; i++ {
		e := &PeerTestGrid{errs: errs, cstate: cstate}
		g, err := NewServer(client, ServerCfg{Namespace: "g1"}, e)
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}
		gridnodes = append(gridnodes, g)
		wg.Add(1)
	}
	go func() {
		<-abort //shutting down...
		if len(errs.errors()) == 0 {
			for _, g := range gridnodes {
				g.Stop() //shutdown complete
			}
		}
	}()

	for _, gg := range gridnodes {
		go func(g *Server) {
			defer wg.Done()
			lis, err := net.Listen("tcp", "localhost:0") //let the OS pick a port for us.
			if err != nil {
				t.Fatalf("listen failed: %v", err)
			}
			// After calling Serve one node within your Grid will be elected as
			// a "leader" and that grid's MakeActor() will be called with a
			// def.Type == "leader".  You can think of this as the main for the
			// grid cluster.
			err = g.Serve(lis)
			if err != nil {
				errs.append(err)
			}
		}(gg)
	}
	//wait for all the cluster's peers to join up.
	cstate.assertEvenualClusterSize(t, int64(len(gridnodes)), 30*time.Second)

	{
		// TC add and remove a peer.
		// Add a new peer and once it's joined and discovered by the leader,
		// then kill it and wait for the leader to discover that it died.
		//
		lis, err := net.Listen("tcp", "localhost:0") //let the OS pick a port for us.
		if err != nil {
			t.Fatalf("listen failed: %v", err)
		}
		e := &PeerTestGrid{errs: errs}
		g, err := NewServer(client, ServerCfg{Namespace: "g1"}, e)
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}
		go func() {
			//wait for the peer to join and be discovered by the leader.
			cstate.assertEvenualClusterSize(t, int64(len(gridnodes)+1), 30*time.Second)
			g.Stop()
		}()
		err = g.Serve(lis)
		if err != nil {
			errs.append(err)
		}
		//wait for the cluster to return to it's orginal size.
		cstate.assertEvenualClusterSize(t, int64(len(gridnodes)), 30*time.Second)
	}

	close(abort)
	wg.Wait()

	if len(errs.errors()) > 0 {
		t.Fatalf("we got errors: %v", errs.errors())
	}
	assert.Equal(t, atomic.LoadInt64(&(cstate.leadersFound)), int64(1))
	assert.Equal(t, atomic.LoadInt64(&(cstate.leadersLost)), int64(1))
	assert.Equal(t, atomic.LoadInt64(&(cstate.peersFound)), int64(1))
	assert.Equal(t, atomic.LoadInt64(&(cstate.peersLost)), int64(1))
}

type PeerTestGrid struct {
	errs   *testerrors
	cstate *clusterState
}

func (e *PeerTestGrid) MakeActor(def *ActorDef) (Actor, error) {
	switch def.Type {
	case "leader":
		return &PeerTestleader{e, e.cstate}, nil
	}
	return nil, fmt.Errorf("unknow actor type: %v", def.Type)
}

type clusterState struct {
	currentPeers int64

	leadersFound int64
	leadersLost  int64
	peersFound   int64
	peersLost    int64
}

func (c *clusterState) assertEvenualClusterSize(t *testing.T, expected int64, timeout time.Duration) {
	st := time.Now()
	addr := &c.currentPeers
	for {
		time.Sleep(100 * time.Millisecond)
		csize := atomic.LoadInt64(addr)
		if csize == expected {
			return
		} else if time.Since(st) >= timeout {
			t.Fatalf("the expected cluster size of %v wasn't reach within %v: final cluster size was:%v", expected, timeout, csize)
			return
		}
	}
}
func (c *clusterState) LeaderFound() {
	atomic.AddInt64(&(c.leadersFound), 1)
}
func (c *clusterState) LeaderLost() {
	atomic.AddInt64(&(c.leadersLost), 1)
}
func (c *clusterState) Peers(cnt int) {
	atomic.AddInt64(&(c.currentPeers), int64(cnt))
}
func (c *clusterState) PeerFound() {
	atomic.AddInt64(&(c.peersFound), 1)
	atomic.AddInt64(&(c.currentPeers), 1)
}
func (c *clusterState) PeerLost() {
	atomic.AddInt64(&(c.peersLost), 1)
	atomic.AddInt64(&(c.currentPeers), -1)
}

type PeerTestleader struct {
	e      *PeerTestGrid
	cstate *clusterState
}

func (a *PeerTestleader) Act(c context.Context) {
	a.cstate.LeaderFound()
	defer a.cstate.LeaderLost()

	client, err := ContextClient(c)
	if err != nil {
		a.e.errs.append(fmt.Errorf("failed to get client:%v", err))
		return
	}

	peers, peersC, err := client.PeersWatch(c)
	if err != nil || len(peers) == 0 {
		a.e.errs.append(fmt.Errorf("failed to get list of peers:%v", err))
		return
	}

	a.cstate.Peers(len(peers))

	for peer := range peersC {
		if peer.Err() != nil {
			fmt.Printf("err: %v\n", peer.Err())
		}
		if peer.Lost() {
			a.cstate.PeerLost()
		}
		if peer.Discovered() {
			a.cstate.PeerFound()
		}
		select {
		case <-c.Done():
			return
		default:
		}
	}
}
