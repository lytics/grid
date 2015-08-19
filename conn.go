package grid

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats"
)

type Conn interface {
	Ready()
	ExitC() <-chan bool
	ReceiveC() <-chan interface{}
	Send(role string, part int, m interface{}) (int, error)
	SendByHashedInt(role string, key int, m interface{}) (int, error)
	SendByHashedString(role string, key string, m interface{}) (int, error)
}

func newDataSet() []interface{} {
	return make([]interface{}, 0, BuffSize/2)
}

type conn struct {
	a         Actor
	g         *grid
	h         hash.Hash64
	receiver  *actorTask
	exit      chan bool
	stoponce  *sync.Once
	readyonce *sync.Once
	intput    chan interface{}
	outputs   map[string]map[int]chan interface{}
}

func newConn(g *grid, receiver *actorTask, a Actor) *conn {
	return &conn{
		a:         a,
		g:         g,
		h:         fnv.New64(),
		receiver:  receiver,
		stoponce:  new(sync.Once),
		readyonce: new(sync.Once),
		exit:      make(chan bool),
		intput:    make(chan interface{}),
		outputs:   make(map[string]map[int]chan interface{}),
	}
}

// ID makes actorHandler a metafora.Task
func (c *conn) ID() string {
	return c.a.ID()
}

// Run + Stop make actorHandler a metafora.Handler
func (c *conn) Run() bool {
	return c.a.Act(c)
}

// Stop + Run make actorHandler a metafora.Handler
func (c *conn) Stop() {
	c.stoponce.Do(func() {
		close(c.exit)
	})
}

func (c *conn) ExitC() <-chan bool {
	return c.exit
}

func (c *conn) ReceiveC() <-chan interface{} {
	return c.intput
}

func (c *conn) Ready() {
	c.readyonce.Do(func() {
		go func() {
			log.Printf("%v: ready to receive", c.receiver.ID())
			for {
				ds := newDataSet()
				err := c.g.natsec.Request(c.receiver.ID(), c.receiver.ID(), &ds, 1*time.Second)
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
	})
}

// Send in the context of this Conn's flow to the given role and part.
func (c *conn) Send(role string, part int, m interface{}) (int, error) {
	actors, ok := c.outputs[role]
	if !ok {
		actors = make(map[int]chan interface{})
		c.outputs[role] = actors
	}
	out, ok := actors[part]
	if !ok {
		out = make(chan interface{}, BuffSize)
		actors[part] = out
		receiver, err := newActorTask(c.receiver.Flow, role, part)
		if err != nil {
			return -1, err
		}
		sub, err := c.g.natsec.QueueSubscribe(receiver.ID(), receiver.ID(), func(m *nats.Msg) {
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
			err := c.g.natsec.Publish(m.Reply, ds)
			if err != nil {
				log.Printf("to: %v, actor: %v, failed to send: %v", m.Reply, receiver, err)
			}
		})
		if err != nil {
			return -1, err
		}
		go func() {
			<-c.exit
			sub.Unsubscribe()
		}()
	}
	out <- m
	return part, nil
}

func (c *conn) SendByHashedInt(role string, key int, m interface{}) (int, error) {
	n, ok := c.g.actorcnt[role]
	if !ok {
		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
	}
	b := make([]byte, binary.MaxVarintLen64)
	len := binary.PutVarint(b, int64(key))
	if len == 0 {
		return -1, fmt.Errorf("failed to binary encode key")
	}
	c.h.Reset()
	c.h.Write(b)
	part := int(c.h.Sum64() % uint64(n))
	return c.Send(role, part, m)
}

func (c *conn) SendByHashedInt32(role string, key int32, m interface{}) (int, error) {
	n, ok := c.g.actorcnt[role]
	if !ok {
		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
	}
	b := make([]byte, binary.MaxVarintLen32)
	len := binary.PutVarint(b, int64(key))
	if len == 0 {
		return -1, fmt.Errorf("failed to binary encode key")
	}
	c.h.Reset()
	c.h.Write(b)
	part := int(c.h.Sum64() % uint64(n))
	return c.Send(role, part, m)
}

func (c *conn) SendByHashedInt64(role string, key int64, m interface{}) (int, error) {
	n, ok := c.g.actorcnt[role]
	if !ok {
		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
	}
	b := make([]byte, binary.MaxVarintLen64)
	len := binary.PutVarint(b, key)
	if len == 0 {
		return -1, fmt.Errorf("failed to binary encode key")
	}
	c.h.Reset()
	c.h.Write(b)
	part := int(c.h.Sum64() % uint64(n))
	return c.Send(role, part, m)
}

func (c *conn) SendByHashedString(role string, key string, m interface{}) (int, error) {
	n, ok := c.g.actorcnt[role]
	if !ok {
		return -1, fmt.Errorf("failed to send because no such role exists: %v", role)
	}
	c.h.Reset()
	c.h.Write([]byte(key))
	part := int(c.h.Sum64() % uint64(n))
	return c.Send(role, part, m)
}
