package main

import (
	"container/ring"

	"github.com/lytics/grid"
)

func NewSenderActor(id string, conf *Conf) grid.Actor {
	return &SenderActor{id: id, conf: conf}
}

type SenderActor struct {
	id   string
	conf *Conf
}

func (a *SenderActor) ID() string {
	return a.id
}

func (a *SenderActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	r := ring.New("counter", a.conf.NrCounters, g)
	n := 0
	for {
		select {
		case <-exit:
			return true
		default:
			if n < a.conf.NrMessages {
				c.Send(r.RouteByModuloInt(n), &CntMsg{From: a.id, Number: n})
			} else {
				c.Send("leader", &DoneMsg{From: a.id})
				return true
			}
		}
	}
}
