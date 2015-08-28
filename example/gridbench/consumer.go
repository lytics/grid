package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
)

func NewConsumerActor(id string, conf *Conf) grid.Actor {
	return &ConsumerActor{id: id, conf: conf}
}

type ConsumerActor struct {
	id   string
	conf *Conf
}

func (a *ConsumerActor) ID() string {
	return a.id
}

func (a *ConsumerActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())

	// Leader will track when all the consumers have exited,
	// and report final answer.
	j := condition.NewJoin(g.Etcd(), 30*time.Second, g.Name(), "consumers", a.id)
	err := j.Join()
	if err != nil {
		log.Fatalf("%v: failed to register: %v", a.id, err)
	}
	defer j.Exit()

	// Report liveness every 15 seconds.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	// Watch the producers. First wait for them to all join.
	// Then report final results when all the producers
	// have exited.
	w := condition.NewCountWatch(g.Etcd(), exit, g.Name(), "producers")
	<-w.WatchUntil(a.conf.NrProducers)

	log.Printf("%v: all producers joined", a.id)

	counts := make(map[string]int)
	for {
		select {
		case <-exit:
			return true
		case <-ticker.C:
			err := j.Alive()
			if err != nil {
				log.Fatalf("%v: failed to report liveness: %v", a.id, err)
			}
		case <-w.WatchError():
			log.Printf("%v: fatal: %v", a.id, err)
			return true
		case <-w.WatchUntil(0):
			for p, n := range counts {
				c.Send("leader", &ResultMsg{Producer: p, Count: n, From: a.id})
			}
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
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case DataMsg:
				log.Printf("%v: rx: %v", a.id, m.Data)
				counts[m.Producer]++
			}
		}
	}
}
