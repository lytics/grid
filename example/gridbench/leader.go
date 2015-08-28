package main

import (
	"log"
	"strings"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
)

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

	w := condition.NewCountWatch(g.Etcd(), exit, g.Name(), "consumers")
	<-w.WatchUntil(a.conf.NrConsumers)

	log.Printf("%v: all consumers joined", a.id)

	ccounts := make(map[string]int)
	pcounts := make(map[string]int)
	pdurations := make(map[string]float64)
	for {
		select {
		case <-exit:
			return true
		case err := <-w.WatchError():
			log.Printf("%v: fatal: %v", a.id, err)
			return true
		case <-w.WatchUntil(0):
			for p, n := range pcounts {
				log.Printf("%v: producer: %v, sent: %v, consumers received: %v, delta: %v, rate: %2.f m/s", a.id, p, n, ccounts[p], n-ccounts[p], float64(n)/pdurations[p])
			}
			return true
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case ResultMsg:
				if strings.Contains(m.From, "producer") {
					pcounts[m.Producer] = m.Count
					pdurations[m.Producer] = m.Duration
				}
				if strings.Contains(m.From, "consumer") {
					ccounts[m.Producer] += m.Count
				}
			}
		}
	}
}
