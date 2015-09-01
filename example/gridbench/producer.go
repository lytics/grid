package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
)

func NewProducerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ProducerActor{def: def, conf: conf}
}

type ProducerActor struct {
	def  *grid.ActorDef
	conf *Conf
}

func (a *ProducerActor) ID() string {
	return a.def.ID()
}

func (a *ProducerActor) Act(g grid.Grid, exit <-chan bool) bool {
	c, err := grid.NewConn(a.ID(), g.Nats())
	if err != nil {
		log.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer c.Close()

	// Make some random length string data.
	data := NewDataMaker(a.conf.MinSize, a.conf.MinCount)
	go data.Start(exit)

	// Consumers will track when all producers exit,
	// and send their final results then.
	j := condition.NewJoin(g.Etcd(), 30*time.Second, g.Name(), "producers", a.ID())
	err = j.Join()
	if err != nil {
		log.Fatalf("%v: failed to register: %v", a.ID(), err)
	}
	defer j.Exit()

	// Report liveness every 15 seconds.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	r := ring.New("consumer", a.conf.NrConsumers, g)
	n := 0
	s := time.Now()
	for {
		select {
		case <-exit:
			return true
		case <-ticker.C:
			err := j.Alive()
			if err != nil {
				log.Fatalf("%v: failed to report liveness: %v", a.ID(), err)
			}
		case <-data.Done():
			err := c.Flush()
			if err != nil {
				log.Fatalf("%v: error: %v", a.ID(), err)
			}
			c.Send("leader", &ResultMsg{Producer: a.ID(), Count: n, From: a.ID(), Duration: time.Now().Sub(s).Seconds()})
			return true
		case d := <-data.Next():
			if n == 1 {
				s = time.Now()
			}
			c.SendBuffered(r.ByInt(n), &DataMsg{Producer: a.ID(), Data: d})
			n++
		}
	}
}
