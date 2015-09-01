package main

import (
	"log"
	"strings"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
)

func NewLeaderActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &LeaderActor{def: def, conf: conf}
}

type LeaderActor struct {
	def  *grid.ActorDef
	conf *Conf
}

func (a *LeaderActor) ID() string {
	return a.def.ID()
}

func (a *LeaderActor) Flow() Flow {
	return Flow(a.def.Settings["flow"])
}

func (a *LeaderActor) Act(g grid.Grid, exit <-chan bool) bool {
	c, err := grid.NewConn(a.ID(), g.Nats())
	if err != nil {
		log.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer c.Close()

	w := condition.NewCountWatch(g.Etcd(), exit, g.Name(), "consumers", a.Flow().Name())
	<-w.WatchUntil(a.conf.NrConsumers)

	log.Printf("%v: all consumers joined", a.ID())

	ccounts := make(map[string]int)
	pcounts := make(map[string]int)
	pdurations := make(map[string]float64)
	for {
		select {
		case <-exit:
			return true
		case err := <-w.WatchError():
			log.Printf("%v: fatal: %v", a.ID(), err)
			return true
		case <-w.WatchUntil(0):
			aggrate := float64(0)
			for p, n := range pcounts {
				rate := float64(n) / pdurations[p]
				aggrate += rate
				log.Printf("%v: producer: %v, sent: %v, consumers received: %v, delta: %v, rate: %2.f m/s", a.ID(), p, n, ccounts[p], n-ccounts[p], rate)
			}
			log.Printf("%v: aggragate rate: %.2f m/s", a.ID(), aggrate)
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
