package grid

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"strings"
	"sync"
	"time"

	"fmt"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/registry"
	"github.com/lytics/retry"
	"google.golang.org/grpc"
)

type clientAndConn struct {
	conn   *grpc.ClientConn
	client WireClient
}

func (cc *clientAndConn) close() error {
	if cc == nil {
		return fmt.Errorf("client and conn is nil")
	}
	return cc.conn.Close()
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
		closeErr := cc.close()
		if closeErr != nil {
			err = closeErr
		}
	}
	return err
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

	req := &Delivery{
		Ver:      Delivery_V1,
		Enc:      Delivery_Gob,
		Data:     buf.Bytes(),
		Receiver: nsReceiver,
	}
	var res *Delivery
	retry.X(3, 1*time.Second, func() bool {
		var client WireClient
		client, err = c.getWireClient(ctx, nsReceiver)
		if err != nil {
			return false
		}
		res, err = client.Process(ctx, req)
		if err != nil && strings.Contains(err.Error(), ErrConnectionIsUnavailable.Error()) {
			// Receiver is on a host that may have died.
			// The error "connection is unavailable"
			// comes from gRPC itself. In such a case
			// it's best to try and replace the client.
			c.deleteClientAndConn(nsReceiver)
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}
		if err != nil && strings.Contains(err.Error(), ErrUnknownMailbox.Error()) {
			// Receiver possibly moved to different
			// host for one reason or another. Get
			// rid of old address and try discovering
			// new host, and send again.
			c.deleteAddress(nsReceiver)
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}
		if err != nil && strings.Contains(err.Error(), ErrReceiverBusy.Error()) {
			// Receiver was busy, ie: the receiving channel
			// was at capacity. Also, the reciever definitely
			// did NOT get the message, so there is no risk
			// of duplication if the request is tried again.
			select {
			case <-ctx.Done():
				return false
			default:
				return true
			}
		}
		return false
	})
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

func (c *Client) deleteAddress(nsReceiver string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.addresses, nsReceiver)
}

func (c *Client) deleteClientAndConn(nsReceiver string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	address, ok := c.addresses[nsReceiver]
	if !ok {
		return
	}

	cc, ok := c.clientsAndConns[address]
	if !ok {
		return
	}
	err := cc.close()
	if err != nil && Logger != nil {
		Logger.Printf("error closing client and connection: %v", err)
	}
	delete(c.clientsAndConns, address)
}
