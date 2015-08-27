package main

import (
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

	w := condition.NewCountWatch(g.Etcd(), exit, g.Name(), "producers")
	<-w.WatchUntil(a.conf.NrProducers)

	counts := make(map[string]int)
	for {
		select {
		case <-exit:
			return true
		case <-w.WatchError():
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
				counts[m.Producer]++
			}
		}
	}
}
