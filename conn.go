package grid

import (
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats"
)

const (
	BuffSize = 8000
)

type Conn interface {
	ReceiveC() <-chan interface{}
	Send(receiver string, m interface{}) error
	Stop()
}

func newDataSet() []interface{} {
	return make([]interface{}, 0, BuffSize/2)
}

type conn struct {
	ec        *nats.EncodedConn
	name      string
	exit      chan bool
	intput    chan interface{}
	outputs   map[string]chan interface{}
	stoponce  *sync.Once
	published chan bool
}

func NewConn(name string, ec *nats.EncodedConn) Conn {
	c := &conn{
		ec:        ec,
		name:      name,
		exit:      make(chan bool),
		intput:    make(chan interface{}),
		outputs:   make(map[string]chan interface{}),
		stoponce:  new(sync.Once),
		published: make(chan bool),
	}
	log.Printf("%v: connected", name)
	go func() {
		for {
			ds := newDataSet()
			err := c.ec.Request(c.name, c.name, &ds, 1*time.Second)
			if err != nil {
				if err.Error() != "nats: Timeout" {
					select {
					case c.intput <- err:
					case <-c.exit:
						return
					}
				}
			} else {
				for _, d := range ds {
					select {
					case c.intput <- d:
					case <-c.exit:
						return
					}
				}
			}
		}
	}()

	return c
}

func (c *conn) ReceiveC() <-chan interface{} {
	return c.intput
}

func (c *conn) Stop() {
	c.stoponce.Do(func() {
		close(c.exit)
	})
}

// Send in the context of this Conn's flow to the given role and part.
func (c *conn) Send(receiver string, m interface{}) error {
	out, ok := c.outputs[receiver]
	if !ok {
		out = make(chan interface{}, BuffSize)
		c.outputs[receiver] = out
		log.Printf("%v: subscribing: %v", c.name, receiver)
		sub, err := c.ec.QueueSubscribe(receiver, receiver, func(m *nats.Msg) {
			tik := time.NewTicker(50 * time.Millisecond)
			defer tik.Stop()
			ds := newDataSet()
			eof := false
			for !eof {
				select {
				case <-tik.C:
					eof = true
				case d := <-out:
					ds = append(ds, d)
					if len(ds) >= BuffSize/2 {
						eof = true
					}
				}
			}
			err := c.ec.Publish(m.Reply, ds)
			if err != nil {
				log.Printf("to: %v, actor: %v, failed to send: %v", m.Reply, receiver, err)
			}
			select {
			case c.published <- true:
			default:
			}
		})
		if err != nil {
			return err
		}
		go func() {
			<-c.exit
			sub.Unsubscribe()
		}()
	}
	out <- m
	return nil
}

// Flush blocks till all queues of this connection are empty.
func (c *conn) Flush() {
	done := true
	for {
		for _, c := range c.outputs {
			if len(c) > 0 {
				done = false
				break
			}
		}
		if done {
			break
		}
		<-c.published
	}
}

// func (c *conn) SendByHashedInt(role string, key int, m interface{}) (int, error) {
// 	n, ok := c.g.actorcnt[role]
// 	if !ok {
// 		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
// 	}
// 	b := make([]byte, binary.MaxVarintLen64)
// 	len := binary.PutVarint(b, int64(key))
// 	if len == 0 {
// 		return -1, fmt.Errorf("failed to binary encode key")
// 	}
// 	c.h.Reset()
// 	c.h.Write(b)
// 	part := int(c.h.Sum64() % uint64(n))
// 	return c.Send(role, part, m)
// }

// func (c *conn) SendByHashedInt32(role string, key int32, m interface{}) (int, error) {
// 	n, ok := c.g.actorcnt[role]
// 	if !ok {
// 		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
// 	}
// 	b := make([]byte, binary.MaxVarintLen32)
// 	len := binary.PutVarint(b, int64(key))
// 	if len == 0 {
// 		return -1, fmt.Errorf("failed to binary encode key")
// 	}
// 	c.h.Reset()
// 	c.h.Write(b)
// 	part := int(c.h.Sum64() % uint64(n))
// 	return c.Send(role, part, m)
// }

// func (c *conn) SendByHashedInt64(role string, key int64, m interface{}) (int, error) {
// 	n, ok := c.g.actorcnt[role]
// 	if !ok {
// 		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
// 	}
// 	b := make([]byte, binary.MaxVarintLen64)
// 	len := binary.PutVarint(b, key)
// 	if len == 0 {
// 		return -1, fmt.Errorf("failed to binary encode key")
// 	}
// 	c.h.Reset()
// 	c.h.Write(b)
// 	part := int(c.h.Sum64() % uint64(n))
// 	return c.Send(role, part, m)
// }

// func (c *conn) SendByHashedString(role string, key string, m interface{}) (int, error) {
// 	n, ok := c.g.actorcnt[role]
// 	if !ok {
// 		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
// 	}
// 	c.h.Reset()
// 	c.h.Write([]byte(key))
// 	part := int(c.h.Sum64() % uint64(n))
// 	return c.Send(role, part, m)
// }
