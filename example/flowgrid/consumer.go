package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
)

func NewConsumerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ConsumerActor{def: def, conf: conf}
}

type ConsumerActor struct {
	def  *grid.ActorDef
	conf *Conf
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

	// Watch the producers. First wait for them to all join.
	// Then report final results when all the producers
	// have exited.
	w := condition.NewCountWatch(g.Etcd(), exit, g.Name(), "producers", a.Flow().Name())
	<-w.WatchUntil(a.conf.NrProducers)
	log.Printf("%v: all producers joined", a.ID())

	// Leader will track when all the consumers have exited,
	// and report final answer.
	j := condition.NewJoin(g.Etcd(), 30*time.Second, g.Name(), "consumers", a.Flow().Name(), a.ID())
	err = j.Join()
	if err != nil {
		log.Fatalf("%v: failed to register: %v", a.ID(), err)
	}
	defer j.Exit()

	// Report liveness every 15 seconds.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	n := 0
	counts := make(map[string]int)
	for {
		select {
		case <-exit:
			return true
		case <-ticker.C:
			err := j.Alive()
			if err != nil {
				log.Fatalf("%v: failed to report liveness: %v", a.ID(), err)
			}
		case <-w.WatchError():
			log.Printf("%v: fatal: %v", a.ID(), err)
			return true
		case <-w.WatchUntil(0):
			for p, n := range counts {
				c.Send(a.Flow().NewFlowName("leader"), &ResultMsg{Producer: p, Count: n, From: a.ID()})
			}
			return true
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case DataMsg:
				counts[m.Producer]++
				n++
				if n%10000 == 0 {
					log.Printf("%v: received: %v", a.ID(), n)
				}
			}
		}
	}
}
