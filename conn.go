package grid

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats"
)

type Ack struct {
	From  string
	Count int
}

type Envelope struct {
	Data []interface{}
}

// Conn is a named bidirectional networked communication channel.
type Conn interface {
	ReceiveC() <-chan interface{}
	Send(receiver string, m interface{}) error
	SendBuffered(receiver string, m interface{}) error
	Flush() error
	Close()
}

type conn struct {
	ec          *nats.EncodedConn
	name        string
	exit        chan bool
	intput      chan interface{}
	outputs     map[string][]interface{}
	stoponce    *sync.Once
	buffsize    int
	sendretries int
	sendtiemout time.Duration
}

// NewConn creates a new connection using NATS as the message transport.
// The connection will receive data sent to the given name. Any NATS
// client can be used.
func NewConn(name string, ec *nats.EncodedConn) (Conn, error) {
	c := &conn{
		ec:          ec,
		name:        name,
		exit:        make(chan bool),
		intput:      make(chan interface{}),
		outputs:     make(map[string][]interface{}),
		stoponce:    new(sync.Once),
		buffsize:    100,
		sendretries: 3,
		sendtiemout: 2 * time.Second,
	}
	log.Printf("%v: connected", name)
	sub0, err := c.ec.QueueSubscribe(c.name, c.name, func(subject, reply string, m *Envelope) {
		for _, d := range m.Data {
			select {
			case <-c.exit:
				return
			case c.intput <- d:
			}
		}
		c.ec.Publish(reply, &Ack{From: c.name, Count: len(m.Data)})
	})
	sub1, err := c.ec.QueueSubscribe(c.name, c.name, func(subject, reply string, m *Envelope) {
		for _, d := range m.Data {
			select {
			case <-c.exit:
				return
			case c.intput <- d:
			}
		}
		c.ec.Publish(reply, &Ack{From: c.name, Count: len(m.Data)})
	})
	if err != nil {
		return nil, err
	}
	go func() {
		<-c.exit
		sub0.Unsubscribe()
		sub1.Unsubscribe()
	}()
	return c, nil
}

// ReceiveC is the channel of inputs for this Conn.
func (c *conn) ReceiveC() <-chan interface{} {
	return c.intput
}

// Send a message to the receiver.
func (c *conn) Send(receiver string, m interface{}) error {
	return c.send(receiver, []interface{}{m})
}

// Send the message and previously buffered messages if the buffer is full,
// otherwise just buffer the message.
func (c *conn) SendBuffered(receiver string, m interface{}) error {
	buf, ok := c.outputs[receiver]
	if !ok {
		buf = make([]interface{}, 0, c.buffsize+100)
		c.outputs[receiver] = buf
	}
	buf = append(buf, m)
	c.outputs[receiver] = buf
	if len(buf) >= c.buffsize {
		err := c.send(receiver, buf)
		if err == nil {
			delete(c.outputs, receiver)
		}
		return err
	}
	return nil
}

// Flush forces the send of all buffered messages.
func (c *conn) Flush() error {
	for receiver, buf := range c.outputs {
		err := c.send(receiver, buf)
		if err == nil {
			delete(c.outputs, receiver)
		} else {
			return err
		}
	}
	return nil
}

// Close.
func (c *conn) Close() {
	c.stoponce.Do(func() {
		close(c.exit)
	})
}

func (c *conn) send(receiver string, ms []interface{}) error {
	var t int
	for ; t < c.sendretries; t++ {
		ack := &Ack{}
		err := c.ec.Request(receiver, &Envelope{Data: ms}, ack, c.sendtiemout)
		if err == nil && ack != nil && ack.From != "" && ack.Count == len(ms) {
			return nil
		}
		if err == nil && ack != nil {
			return fmt.Errorf("received bad ack from: %v, count: %v, expected ack from: %v, count: %v", ack.From, ack.Count, receiver, len(ms))
		}
		if err == nil && ack == nil {
			return fmt.Errorf("received no ack from: %v", ack.From)
		}
		if err != nil && err.Error() != "nats: Timeout" {
			return err
		}
		select {
		case <-c.exit:
			return fmt.Errorf("failed to send before exit requested")
		default:
			// Exit not wanted, try again.
		}
	}
	return fmt.Errorf("failed to send after %d attempts with total time: %s", t, time.Duration(t)*c.sendtiemout)
}

// SetConnBuffSize change the size of internal buffers, used by SendBuffered
// function, to the given size. The default is 100.
func SetConnBuffSize(c Conn, size int) {
	switch c := c.(type) {
	case *conn:
		if size < 0 {
			c.buffsize = 1
		} else {
			c.buffsize = size
		}
	}
}

// SetConnSendTimeout changes the timeout of send operations. Setting
// this low, while at the same time using a large buffer size with
// large messages, may cause sends to error due to insufficient
// time to send all data. The default is 2 seconds.
func SetConnSendTimeout(c Conn, timeout time.Duration) {
	switch c := c.(type) {
	case *conn:
		c.sendtiemout = timeout
	}
}

// SetConnSendRetries changes the number of attempts to resend data.
// The total number of send attempts will be 1 + n. The default
// is 3 retries.
func SetConnSendRetries(c Conn, n int) {
	switch c := c.(type) {
	case *conn:
		if n < 0 {
			c.sendretries = 1
		} else {
			c.sendretries = 1 + n
		}
	}
}
