package grid

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"sync"
	"time"

	"fmt"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/registry"
	"google.golang.org/grpc"
)

type changeType int

const (
	entityErr        changeType = 0
	entityDiscovered changeType = 1
	entityLost       changeType = 2
)

type clientAndConn struct {
	conn   *grpc.ClientConn
	client WireClient
}

// QueryResult indicating that a peer has been discovered,
// lost, or some error has occured with a peer or the watch
// of peers.
type QueryResult struct {
	entity string
	err    error
	change changeType
}

// Entity name that caused the event.
func (p *QueryResult) Entity() string {
	return p.entity
}

// Discovered entity.
func (p *QueryResult) Discovered() bool {
	return p.change == entityDiscovered
}

// Lost entity.
func (p *QueryResult) Lost() bool {
	return p.change == entityLost
}

// Err caught watching peers. The error is not
// associated with any particular peer.
func (p *QueryResult) Err() error {
	return p.err
}

// String representation of peer change.
func (p *QueryResult) String() string {
	switch p.change {
	case entityLost:
		return fmt.Sprintf("entity change: lost: %v", p.entity)
	case entityDiscovered:
		return fmt.Sprintf("entity change: discovered: %v", p.entity)
	default:
		return fmt.Sprintf("entity change: error: %v", p.err)
	}
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

// NewClient using the given etcd client and configuration.
func NewClient(etcd *etcdv3.Client, cfg ClientCfg) (*Client, error) {
	setClientCfgDefaults(&cfg)

	r, err := registry.New(etcd)
	if err != nil {
		return nil, err
	}
	r.Timeout = cfg.Timeout

	return &Client{
		cfg:             cfg,
		registry:        r,
		addresses:       make(map[string]string),
		clientsAndConns: make(map[string]*clientAndConn),
	}, nil
}

// Close all outbound connections of this client immediately.
func (c *Client) Close() error {
	var err error
	for _, cc := range c.clientsAndConns {
		clostErr := cc.conn.Close()
		if clostErr != nil {
			err = clostErr
		}
	}
	return err
}

// QueryWatch monitors the entry and exit of entities in the same namespace.
// Where entities are either peers, actors, or mailboxes.
//
// Example usage:
//
//     client, err := grid.NewClient(...)
//     ...
//
//     currentpeers, watch, err := client.QueryWatch(c, grid.Peers)
//     ...
//
//     for _, peer := range currentpeers {
//         // Do work regarding peer.
//     }
//
//     for event := range watch {
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
func (c *Client) QueryWatch(ctx context.Context, filter entityType) ([]string, <-chan *QueryResult, error) {
	nsName, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, nil, err
	}

	regs, changes, err := c.registry.Watch(ctx, nsName)
	ents := nameFromRegs(filter, c.cfg.Namespace, regs)

	queryResults := make(chan *QueryResult)
	put := func(change *QueryResult) {
		select {
		case <-ctx.Done():
		case queryResults <- change:
		}
	}
	go func() {
		defer close(queryResults)
		for {
			select {
			case <-ctx.Done():
				return
			case change, open := <-changes:
				if !open {
					put(&QueryResult{err: ErrWatchClosedUnexpectedly})
					return
				}
				if change.Error != nil {
					put(&QueryResult{err: err})
					return
				}
				switch change.Type {
				case registry.Delete:
					ent := nameFromReg(filter, c.cfg.Namespace, change.Reg)
					put(&QueryResult{entity: ent, change: entityLost})
				case registry.Create, registry.Modify:
					ent := nameFromReg(filter, c.cfg.Namespace, change.Reg)
					put(&QueryResult{entity: ent, change: entityDiscovered})
				}
			}
		}
	}()

	return ents, queryResults, nil
}

// Query in this client's namespace. The filter can be any one of
// Peers, Actors, or Mailboxes.
func (c *Client) Query(timeout time.Duration, filter entityType) ([]string, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.QueryC(timeoutC, filter)
}

// QueryC (query) in this client's namespace. The filter can be any
// one of Peers, Actors, or Mailboxes. The context can be used to
// control cancelation or timeouts.
func (c *Client) QueryC(ctx context.Context, filter entityType) ([]string, error) {
	nsPrefix, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, err
	}
	regs, err := c.registry.FindRegistrations(ctx, nsPrefix)
	if err != nil {
		return nil, err
	}

	names := nameFromRegs(filter, c.cfg.Namespace, regs)
	return names, nil
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
	nsReceiver, err := namespaceName(Mailboxes, c.cfg.Namespace, receiver)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(env)
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
		if err != nil && err == registry.ErrUnknownKey {
			return nil, ErrUnknownMailbox
		}
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

// minus of sets a and b, in mathematical notation: A \ B,
// ie: all elements in A that are not in B.
//
// See: https://www.techonthenet.com/sql/minus.php
//
// Example:
//    lostPeers := difference(oldPeers, currentPeers)
//    discoveredPeers := difference(currentPeers, oldPeers)
//
func minus(a map[string]struct{}, b map[string]struct{}) map[string]struct{} {
	res := map[string]struct{}{}
	for in := range a {
		if _, skip := b[in]; !skip {
			res[in] = struct{}{}
		}
	}
	return res
}

func nameFromRegs(filter entityType, namespace string, regs []*registry.Registration) []string {
	peers := make([]string, 0)
	for _, reg := range regs {
		peer := nameFromReg(filter, namespace, reg)
		peers = append(peers, peer)
	}
	return peers
}

func nameFromReg(filter entityType, namespace string, reg *registry.Registration) string {
	peer, err := stripNamespace(filter, namespace, reg.Key)
	// INVARIANT
	// Under all circumstances if a registration is returned
	// from the prefix scan above, ie: FindRegistrations,
	// then each registration must contain the namespace
	// as a prefix of the key.
	if err != nil {
		panic("registry key without proper namespace prefix: " + reg.Key)
	}
	return peer
}
