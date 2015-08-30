package grid

import (
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats"
)

const BuffSize = 200

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
	ec       *nats.EncodedConn
	name     string
	exit     chan bool
	intput   chan interface{}
	outputs  map[string][]interface{}
	stoponce *sync.Once
}

func NewConn(name string, ec *nats.EncodedConn) (Conn, error) {
	c := &conn{
		ec:       ec,
		name:     name,
		exit:     make(chan bool),
		intput:   make(chan interface{}, BuffSize),
		outputs:  make(map[string][]interface{}),
		stoponce: new(sync.Once),
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

// Send a message if the buffer is full, otherwise just buffer the message.
func (c *conn) SendBuffered(receiver string, m interface{}) error {
	buf, ok := c.outputs[receiver]
	if !ok {
		buf = make([]interface{}, 0, BuffSize)
		c.outputs[receiver] = buf
	}
	buf = append(buf, m)
	c.outputs[receiver] = buf
	if len(buf) >= BuffSize {
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
	for {
		ack := &Ack{}
		err := c.ec.Request(receiver, &Envelope{Data: ms}, ack, 2*time.Second)
		if err == nil && ack != nil && ack.From != "" && ack.Count == len(ms) {
			return nil
		}
		if err.Error() != "nats: Timeout" {
			return err
		}
		select {
		case <-c.exit:
			return nil
		default:
		}
	}
}
