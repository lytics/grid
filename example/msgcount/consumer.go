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

	j := condition.NewJoinWatch(g.Etcd(), exit, g.Name(), "leader")

	counts := make(map[string]int)
	for {
		select {
		case <-exit:
			return true
		case <-j.WatchExit():
			return true
		case <-j.WatchError():
			return true
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case DataMsg:
				counts[m.From]++
			case SendResultMsg:
				// There is a cycle in communication
				// between leader and us, don't
				// block on the send.
				go c.Send("leader", &ResultMsg{Producer: m.Producer, Count: counts[m.Producer]})
			}
		}
	}
}
