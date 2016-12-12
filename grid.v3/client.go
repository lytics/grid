package grid

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/registry"
	"google.golang.org/grpc"
)

type clientAndConn struct {
	conn   *grpc.ClientConn
	client WireClient
}

// Client for grid-actors or non-actors to make requests to grid-actors.
// The client can be used by multiple go-routines.
type Client struct {
	mu              sync.Mutex
	registry        *registry.Registry
	namespace       string
	addresses       map[string]string
	clientsAndConns map[string]*clientAndConn
}

// NewClient with namespace and using the given etcd client.
func NewClient(etcd *etcdv3.Client, r *registry.Registry, namespace string) (*Client, error) {
	return &Client{
		registry:        r,
		namespace:       namespace,
		addresses:       make(map[string]string),
		clientsAndConns: make(map[string]*clientAndConn),
	}, nil
}

// Close all outbound connections of this client immediately.
func (c *Client) Close() error {
	c.registry.Stop()

	var err error
	for _, cc := range c.clientsAndConns {
		err = cc.conn.Close()
	}

	return err
}

type PeerChangeEvent struct {
	peer        string
	err         error
	stateChange int
}

func (p *PeerChangeEvent) Peer() string {
	return p.peer
}

func (p *PeerChangeEvent) Discovered() bool {
	return p.stateChange == PeerDiscovered
}

func (p *PeerChangeEvent) Lost() bool {
	return p.stateChange == PeerLost
}
func (p *PeerChangeEvent) Err() error {
	return p.err
}

const (
	WatchErr       = -1
	PeerDiscovered = 1
	PeerLost       = 2
)

/*

	client := grid.Client(...)
	watch, currentpeers := client.PeersWatch(c)
	for _, peer := range currentpeers {
			//do work or request metadata from peer
	}
	for peer := range watch.C {
			if peer.Err() != nil {
				//deal with a peer that went down.. reschedule actors..
			}
			if peer.Lost() {
				//deal with a peer that went down.. reschedule actors..
			}
			if peer.Discovered() {
				//Request metadata from the peer.
				//deal with a new peer, maybe rebalance some existing actors on to it or send new tasks to it.
			}
	}
*/
func (c *Client) PeersWatch(ctx context.Context) ([]string, <-chan *PeerChangeEvent, error) {
	peers, err := c.PeersC(ctx)
	if err != nil {
		return nil, nil, err
	}

	currentPeers := map[string]struct{}{}
	for _, p := range peers {
		currentPeers[p] = struct{}{}
	}

	watchchan := make(chan *PeerChangeEvent)
	go func() {
		defer close(watchchan)
		//TODO consider switching this to using an etcd watch? But that maybe overkill and more complex?
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		oldPeers := currentPeers
		for {
			select {
			case <-ticker.C:
				peers, err := c.PeersC(ctx)
				if err != nil {
					select {
					case watchchan <- &PeerChangeEvent{"", err, WatchErr}:
					case <-ctx.Done():
					}
					return
				}

				currentPeers := map[string]struct{}{}
				for _, p := range peers {
					currentPeers[p] = struct{}{}
				}
				lostPeers := minus(oldPeers, currentPeers)
				discoveredPeers := minus(currentPeers, oldPeers)
				oldPeers = currentPeers

				for peer := range lostPeers {
					watchchan <- &PeerChangeEvent{peer, nil, PeerLost}
				}
				for peer := range discoveredPeers {
					watchchan <- &PeerChangeEvent{peer, nil, PeerDiscovered}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return peers, watchchan, nil
}

// minus creeates a set out of two existing sets.
// Where the new set is the set of elements which only belong to first set.  i.e.
// set A minus set B.
// for example using the let of peers as an example:
//    lostPeers := minus(oldPeers, currentPeers)
//    discoveredPeers := minus(currentPeers, oldPeers)
//
func minus(a map[string]struct{}, b map[string]struct{}) map[string]struct{} {
	res := map[string]struct{}{}
	for in, _ := range a {
		if _, skip := b[in]; !skip {
			res[in] = struct{}{}
		}
	}
	return res
}

// Peers in this client's namespace. A peer is any process that called
// the Serve method to act as a server for the namespace.
func (c *Client) Peers(timeout time.Duration) ([]string, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.PeersC(timeoutC)
}

// PeersC (peers) in this client's namespace. A peer is any process that called
// the Serve method to act as a server for the namespace. The context can be
// used to control cancelation or timeouts.
func (c *Client) PeersC(ctx context.Context) ([]string, error) {
	regs, err := c.registry.FindRegistrations(ctx, "grid-"+c.namespace)
	if err != nil {
		return nil, err
	}

	peers := make([]string, 0)
	for _, reg := range regs {
		peers = append(peers, reg.Key)
	}

	return peers, nil
}

// Request a response for the given message.
func (c *Client) Request(timeout time.Duration, receiver string, msg interface{}) (interface{}, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.RequestC(timeoutC, receiver, msg)
}

// RequestC (request) a response for the given message. The context can be
// used to control cancelation or timeouts.
func (c *Client) RequestC(ctx context.Context, receiver string, msg interface{}) (interface{}, error) {
	env := &envelope{
		Msg: msg,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(env)
	if err != nil {
		return nil, err
	}

	client, err := c.getWireClient(ctx, receiver)
	if err != nil {
		return nil, err
	}

	req := &Delivery{
		Ver:      Delivery_V1,
		Enc:      Delivery_Gob,
		Data:     buf.Bytes(),
		Receiver: receiver,
	}
	res, err := client.Process(ctx, req)
	if err != nil {
		return nil, err
	}

	buf.Reset()
	n, err := buf.Write(res.Data)
	if err != nil {
		return nil, err
	}
	if n != len(res.Data) {
		return nil, io.ErrUnexpectedEOF
	}

	env = &envelope{}
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(env)
	if err != nil {
		return nil, err
	}

	if env.Msg != nil {
		return env.Msg, nil
	}
	return nil, ErrNilResponse
}

// getWireClient for the address of the receiver.
func (c *Client) getWireClient(ctx context.Context, receiver string) (WireClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	address, ok := c.addresses[receiver]
	if !ok {
		reg, err := c.registry.FindRegistration(ctx, receiver)
		if err != nil {
			return nil, err
		}
		address = reg.Address
		c.addresses[receiver] = address
	}

	cc, ok := c.clientsAndConns[address]
	if !ok {
		conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(20*time.Second), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
		client := NewWireClient(conn)
		cc = &clientAndConn{
			conn:   conn,
			client: client,
		}
		c.clientsAndConns[address] = cc
	}
	return cc.client, nil
}
