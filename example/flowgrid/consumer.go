package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
)

func NewConsumerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ConsumerActor{def: def, conf: conf, counts: make(map[string]int)}
}

type ConsumerActor struct {
	def    *grid.ActorDef
	conf   *Conf
	counts map[string]int
}

func (a *ConsumerActor) ID() string {
	return a.def.ID()
}

func (a *ConsumerActor) Flow() Flow {
	return Flow(a.def.Settings["flow"])
}

func (a *ConsumerActor) Act(g grid.Grid, exit <-chan bool) bool {
	c, err := grid.NewConn(a.ID(), g.Nats())
	if err != nil {
		log.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer c.Close()
	defer a.SendCounts(c)

	// Watch the producers. First wait for them to all join.
	// Then report final results when all the producers
	// have exited.
	w := condition.NewCountWatch(g.Etcd(), exit, g.Name(), "producers", a.Flow().Name())
	<-w.WatchUntil(a.conf.NrProducers)
	log.Printf("%v: all producers joined", a.ID())

	// Leader will track when all the consumers have exited,
	// and report final answer.
	j := condition.NewJoin(g.Etcd(), 5*time.Minute, g.Name(), "consumers", a.Flow().Name(), a.ID())
	err = j.Join()
	if err != nil {
		log.Fatalf("%v: failed to register: %v", a.ID(), err)
	}
	defer j.Exit()

	// Report liveness.
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	n := 0
	chaos := NewChaos()
	for {
		select {
		case <-exit:
			return true
		case <-ticker.C:
			err := j.Alive()
			if err != nil {
				log.Printf("%v: failed to report liveness: %v", a.ID(), err)
				return false
			}
			if chaos.Roll() {
				return false
			}
		case <-w.WatchError():
			log.Printf("%v: fatal: %v", a.ID(), err)
			return true
		case <-w.WatchUntil(0):
			return true
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case DataMsg:
				a.counts[m.Producer]++
				n++
				if n%10000 == 0 {
					log.Printf("%v: received: %v", a.ID(), n)
					a.SendCounts(c)
				}
			}
		}
	}
}

func (a *ConsumerActor) SendCounts(c grid.Conn) {
	for p, n := range a.counts {
		err := c.Send(a.Flow().NewFlowName("leader"), &ResultMsg{Producer: p, Count: n, From: a.ID()})
		if err != nil {
			delete(a.counts, p)
		}
	}
}
