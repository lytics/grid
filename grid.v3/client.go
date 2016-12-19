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

type peerWatchDelta int

const (
	peerErr        peerWatchDelta = 0
	peerDiscovered peerWatchDelta = 1
	peerLost       peerWatchDelta = 2
)

type clientAndConn struct {
	conn   *grpc.ClientConn
	client WireClient
}

// PeerChangeEvent indicating that a peer has been discovered,
// lost, or some error has occured with a peer or the watch
// of peers.
type PeerChangeEvent struct {
	peer        string
	err         error
	stateChange peerWatchDelta
}

// Peer name that caused the event.
func (p *PeerChangeEvent) Peer() string {
	return p.peer
}

// Discovered peer.
func (p *PeerChangeEvent) Discovered() bool {
	return p.stateChange == peerDiscovered
}

// Lost peer.
func (p *PeerChangeEvent) Lost() bool {
	return p.stateChange == peerLost
}

// Err caught watching peers. The error is not
// associated with any particular peer.
func (p *PeerChangeEvent) Err() error {
	return p.err
}

// Client for grid-actors or non-actors to make requests to grid-actors.
// The client can be used by multiple go-routines.
type Client struct {
	mu              sync.Mutex
	cfg             ClientCfg
	registry        *registry.Registry
	addresses       map[string]string
	clientsAndConns map[string]*clientAndConn
}

// NewClient with namespace and using the given etcd client.
func NewClient(etcd *etcdv3.Client, cfg ClientCfg) (*Client, error) {
	setClientCfgDefaults(&cfg)

	r, err := registry.New(etcd)
	if err != nil {
		return nil, err
	}
	r.Timeout = cfg.Timeout
	r.LeaseDuration = cfg.LeaseDuration

	return &Client{
		cfg:             cfg,
		registry:        r,
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

// PeersWatch monitors the entry and exit of peers in the same namespace.
// Example usage:
//
//     client, err := grid.NewClient(...)
//     ...
//
//     watch, currentpeers := client.PeersWatch(c)
//     for _, peer := range currentpeers {
//         // Do work regarding peer.
//     }
//
//     for event := range watch.C {
//         if event.Err() != nil {
//             // Error occured watching peers, deal with error.
//         }
//         if event.Lost() {
//             // Existing peer lost, reschedule work on extant peers.
//         }
//         if event.Discovered() {
//             // New peer discovered, assign work, get data, reschedule, etc.
//         }
//     }
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
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		oldPeers := currentPeers
		for {
			select {
			case <-ticker.C:
				peers, err := c.PeersC(ctx)
				if err != nil {
					select {
					case watchchan <- &PeerChangeEvent{err: err}:
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
					watchchan <- &PeerChangeEvent{peer, nil, peerLost}
				}
				for peer := range discoveredPeers {
					watchchan <- &PeerChangeEvent{peer, nil, peerDiscovered}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return peers, watchchan, nil
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
	regs, err := c.registry.FindRegistrations(ctx, c.cfg.Namespace+"-grid-")
	if err != nil {
		return nil, err
	}

	peers := make([]string, 0)
	for _, reg := range regs {
		prefix := c.cfg.Namespace + "-"
		// INVARIANT
		// Under all circumstances if a registration is returned
		// from the prefix scan above, ie: FindRegistrations,
		// then each registration must contain the namespace
		// as a prefix with the key.
		if len(reg.Key) <= len(prefix) {
			panic("registry key without proper namespace prefix")
		}
		peers = append(peers, reg.Key[len(prefix):])
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

	// Namespaced receiver name.
	nsReceiver := c.cfg.Namespace + "-" + receiver

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(env)
	if err != nil {
		return nil, err
	}

	client, err := c.getWireClient(ctx, nsReceiver)
	if err != nil {
		return nil, err
	}

	req := &Delivery{
		Ver:      Delivery_V1,
		Enc:      Delivery_Gob,
		Data:     buf.Bytes(),
		Receiver: nsReceiver,
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
func (c *Client) getWireClient(ctx context.Context, nsReceiver string) (WireClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	address, ok := c.addresses[nsReceiver]
	if !ok {
		reg, err := c.registry.FindRegistration(ctx, nsReceiver)
		if err != nil {
			return nil, err
		}
		address = reg.Address
		c.addresses[nsReceiver] = address
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

// minus creeates a set out of two existing sets.
// Where the new set is the set of elements which
// only belong to the first set.
//
// Example:
//    lostPeers := minus(oldPeers, currentPeers)
//    discoveredPeers := minus(currentPeers, oldPeers)
func minus(a map[string]struct{}, b map[string]struct{}) map[string]struct{} {
	res := map[string]struct{}{}
	for in, _ := range a {
		if _, skip := b[in]; !skip {
			res[in] = struct{}{}
		}
	}
	return res
}
