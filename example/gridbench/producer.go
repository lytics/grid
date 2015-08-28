package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
)

func NewProducerActor(id string, conf *Conf) grid.Actor {
	return &ProducerActor{id: id, conf: conf}
}

type ProducerActor struct {
	id   string
	conf *Conf
}

func (a *ProducerActor) ID() string {
	return a.id
}

func (a *ProducerActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	r := ring.New("consumer", a.conf.NrConsumers, g)

	// Make some random length string data.
	data := NewDataMaker(a.conf.MinSize, a.conf.MinCount)
	go data.Start(exit)

	// Consumers will track when all producers exit,
	// and send their final results then.
	j := condition.NewJoin(g.Etcd(), 30*time.Second, g.Name(), "producers", a.id)
	err := j.Join()
	if err != nil {
		log.Fatalf("%v: failed to register: %v", a.id, err)
	}
	defer j.Exit()

	// Report liveness every 15 seconds.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	n := 0
	for {
		select {
		case <-exit:
			return true
		case <-ticker.C:
			err := j.Alive()
			if err != nil {
				log.Fatalf("%v: failed to report liveness: %v", a.id, err)
			}
		case <-data.Done():
			c.Send("leader", &ResultMsg{Producer: a.id, Count: n, From: a.id})
			for {
				select {
				case <-exit:
					return true
				case <-c.Published():
					if c.Size() == 0 {
						return true
					}
				}
			}
		case d := <-data.Next():
			c.Send(r.ByInt(n), &DataMsg{Producer: a.id, Data: d})
			n++
		}
	}
}
