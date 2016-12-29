package grid

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

var clientCfg = ClientCfg{
	Namespace: "g1",
}

var serverCfg = ServerCfg{
	Namespace:     "g1",
	LeaseDuration: 10 * time.Second,
}

func TestPeersWatch(t *testing.T) {
	var cstate = &clusterState{}

	etcd, cleanup := bootstrap(t)
	defer cleanup()
	errs := &testerrors{}

	client, err := NewClient(etcd, clientCfg)
	if err != nil {
		t.Fatal(err)
	}

	abort := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	gridnodes := []*Server{}
	for i := 0; i < 4; i++ {
		e := &PeerTestGrid{errs: errs, cstate: cstate, client: client}
		g, err := NewServer(etcd, serverCfg, e)
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
	cstate.assertEvenualClusterSize(t, len(gridnodes), 30*time.Second)

	{
		// TC add and remove a peer.
		// Add a new peer and once it's joined and discovered by the leader,
		// then kill it and wait for the leader to discover that it died.
		//
		lis, err := net.Listen("tcp", "localhost:0") //let the OS pick a port for us.
		if err != nil {
			t.Fatalf("listen failed: %v", err)
		}
		e := &PeerTestGrid{errs: errs, client: client}
		g, err := NewServer(etcd, serverCfg, e)
		if err != nil {
			t.Fatalf("NewServer failed: %v", err)
		}
		go func() {
			//wait for the peer to join and be discovered by the leader.
			cstate.assertEvenualClusterSize(t, len(gridnodes)+1, 30*time.Second)
			g.Stop()
		}()
		err = g.Serve(lis)
		if err != nil {
			errs.append(err)
		}
		//wait for the cluster to return to it's orginal size.
		cstate.assertEvenualClusterSize(t, len(gridnodes), 30*time.Second)
	}

	close(abort)
	wg.Wait()

	if len(errs.errors()) > 0 {
		t.Fatalf("we got errors: %v", errs.errors())
	}
	assert.Equal(t, cstate.leadersFound, 1)
	assert.Equal(t, cstate.leadersLost, 1)
	assert.Equal(t, cstate.peersFound, 1)
	assert.Equal(t, cstate.peersLost, 1)
}

type PeerTestGrid struct {
	errs   *testerrors
	cstate *clusterState
	client *Client
}

func (e *PeerTestGrid) MakeActor(def *ActorDef) (Actor, error) {
	switch def.Type {
	case "leader":
		return &PeerTestleader{e}, nil
	}
	return nil, fmt.Errorf("unknow actor type: %v", def.Type)
}

type clusterState struct {
	mu sync.Mutex

	currentPeers int
	leadersFound int
	leadersLost  int
	peersFound   int
	peersLost    int
}

func (c *clusterState) assertEvenualClusterSize(t *testing.T, expected int, timeout time.Duration) {
	st := time.Now()
	for {
		time.Sleep(100 * time.Millisecond)
		cnt := c.CurrentPeersCount()
		if cnt == expected {
			return
		} else if time.Since(st) >= timeout {
			t.Fatalf("the expected cluster size of %v wasn't reach within %v: final cluster size was:%v", expected, timeout, cnt)
			return
		}
	}
}

func (c *clusterState) CurrentPeersCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.currentPeers
}

func (c *clusterState) LeaderFound() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.leadersFound++
}
func (c *clusterState) LeaderLost() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.leadersLost++
}
func (c *clusterState) Peers(cnt int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.currentPeers += cnt
}

func (c *clusterState) PeerFound() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peersFound++
	c.currentPeers++
}
func (c *clusterState) PeerLost() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.peersLost++
	c.currentPeers--
}

type PeerTestleader struct {
	e *PeerTestGrid
}

func (a *PeerTestleader) Act(c context.Context) {
	a.e.cstate.LeaderFound()
	defer a.e.cstate.LeaderLost()

	peers, peersC, err := a.e.client.PeersWatch(c)
	if err != nil || len(peers) == 0 {
		a.e.errs.append(fmt.Errorf("failed to get list of peers:%v", err))
		return
	}

	a.e.cstate.Peers(len(peers))
	for peer := range peersC {
		if peer.Err() != nil {
			fmt.Printf("err: %v\n", peer.Err())
		}
		if peer.Lost() {
			a.e.cstate.PeerLost()
		}
		if peer.Discovered() {
			a.e.cstate.PeerFound()
		}
		select {
		case <-c.Done():
			return
		default:
		}
	}
}
