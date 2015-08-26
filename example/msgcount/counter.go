package main

import "github.com/lytics/grid"

func NewCounterActor(id string, conf *Conf) grid.Actor {
	return &CounterActor{id: id, conf: conf}
}

type CounterActor struct {
	id   string
	conf *Conf
}

func (a *CounterActor) ID() string {
	return a.id
}

func (a *CounterActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	counts := make(map[string]int)
	for {
		select {
		case <-exit:
			return true
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case CntMsg:
				counts[m.From]++
			}
		}
	}
}
