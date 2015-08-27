package main

import "github.com/lytics/grid"

func NewLeaderActor(id string, conf *Conf) grid.Actor {
	return &LeaderActor{id: id, conf: conf}
}

type LeaderActor struct {
	id   string
	conf *Conf
}

func (a *LeaderActor) ID() string {
	return a.id
}

func (a *LeaderActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	for {
		select {
		case <-exit:
			return true
		case m := <-c.ReceiveC():
			switch m.(type) {
			case ResultMsg:
			}
		}
	}
}
