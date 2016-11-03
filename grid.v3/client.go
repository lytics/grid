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

// Client for non-actors to talk with grid actors.
type Client struct {
	mu              sync.Mutex
	registry        *registry.Registry
	namespace       string
	addresses       map[string]string
	clientsAndConns map[string]*clientAndConn
}

// NewClient withing namespace and using the etcd client.
func NewClient(etcd *etcdv3.Client, namespace string) (*Client, error) {
	r, err := registry.New(etcd)
	if err != nil {
		return nil, err
	}
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

// Peers in this client's namespace. A peer is any process that called
// the grid Server Serve method.
func (c *Client) Peers(timeout time.Duration) ([]string, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.PeersC(timeoutC)
}

// PeersC in this client's namespace. A peer is any process that called
// the grid Server Serve method.
func (c *Client) PeersC(ctx context.Context) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

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

// Request a response for the message. The response message is in the returned envelope.
func (c *Client) Request(timeout time.Duration, receiver string, msg interface{}) (*Envelope, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.RequestC(timeoutC, receiver, msg)
}

// RequestC a response for the message. The response message is in the returned envelope.
func (c *Client) RequestC(ctx context.Context, receiver string, msg interface{}) (*Envelope, error) {
	env := &Envelope{
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

	env = &Envelope{}
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(env)
	if err != nil {
		return nil, err
	}

	return env, nil
}

// getWireClient to the address of the receiver.
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
