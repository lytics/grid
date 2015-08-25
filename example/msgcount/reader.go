package main

import (
	"fmt"
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
)

func NewReaderActor(id string, conf *Conf) grid.Actor {
	return &ReaderActor{id: id, conf: conf}
}

type ReaderActor struct {
	id   string
	conf *Conf
}

func (a *ReaderActor) ID() string {
	return a.id
}

func (a *ReaderActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	n := 0

	countersready := condition.ActorJoin(g.Etcd(), exit, a.conf.GridName, "counter", a.conf.NrCounters)
	select {
	case <-countersready:
		log.Printf("%v: counters are now running", a.id)
	case <-exit:
		return true
	}

	start := time.Now()
	g.Etcd().Delete(a.StatePath(), false)
	for {
		select {
		case <-exit:
			return true
		default:
			if n < a.conf.NrMessages {
				err := c.Send(a.ByModulo("counter", n), &CntMsg{From: a.id, Number: n})
				if err != nil {
					log.Fatalf("%v: error: %v", a.id, err)
				}
				n++
				if n%100000 == 0 {
					log.Printf("%v: sent: %v, rate: %.2f/sec", a.id, n, float64(n)/time.Now().Sub(start).Seconds())
				}
			} else {
				g.Etcd().Set(a.StatePath(), "done", 100)
				return true
			}
		}
	}
}

func (a *ReaderActor) ByModulo(role string, key int) string {
	part := key % a.conf.NrCounters
	return fmt.Sprintf("%s.%s.%d", a.conf.GridName, role, part)
}

func (a *ReaderActor) ByNumber(role string, part int) string {
	return fmt.Sprintf("%s.%s.%d", a.conf.GridName, role, part)
}

func (a *ReaderActor) StatePath() string {
	return fmt.Sprintf("/%v/state/%v", a.conf.GridName, a.id)
}
