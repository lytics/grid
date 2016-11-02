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

type Client struct {
	mu              sync.Mutex
	registry        *registry.Registry
	namespace       string
	addresses       map[string]string
	clientsAndConns map[string]*clientAndConn
}

func NewClient(namespace string, etcd *etcdv3.Client) (*Client, error) {
	r, err := registry.New(etcd)
	if err != nil {
		return nil, err
	}
	return &Client{
		registry:  r,
		namespace: namespace,
	}, nil
}

func (c *Client) Close() {
	c.registry.Stop()
}

func (c *Client) Peers(timeout time.Duration) ([]string, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.PeersC(timeoutC)
}

// Peers in the given namespace. The names can be used in
// RequestActorStart to start actors on a peer.
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

// Request a response for the given message. The response is in the returned envelope.
func (c *Client) Request(timeout time.Duration, receiver string, msg interface{}) (*Envelope, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.RequestC(timeoutC, receiver, msg)
}

// RequestC a response for the given message. The response is in the returned envelope.
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
