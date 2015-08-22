package main

import (
	"log"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/grid"
)

func NewReaderActor(id string, nmessages, ncounters int) grid.Actor {
	return &ReaderActor{id: id, nmessages: nmessages, ncounters: ncounters}
}

type ReaderActor struct {
	id        string
	ncounters int
	nmessages int
}

func (a *ReaderActor) ID() string {
	return a.id
}

func (a *ReaderActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	n := 0
	start := time.Now()

	stateexit := make(chan bool)
	stateinfo := make(chan *etcd.Response)
	go g.Etcd().Watch(GridName, 0, true, stateinfo, stateexit)
	defer close(stateexit)

	condchan, condexit := ConditionActorRunning(g.Etcd(), GridName, "counter", a.ncounters)
	defer close(condexit)

	select {
	case <-condchan:
		log.Printf("%v: condition is now true", a.id)
	case <-exit:
		return true
	}

	for {
		select {
		case <-exit:
			return true
		default:
			if n < a.nmessages {
				err := c.Send(ByModuloInt("counter", n, a.ncounters), &CntMsg{From: a.id, Number: n})
				if err != nil {
					log.Fatalf("%v: error: %v", a.id, err)
				}
				n++
				if n%100000 == 0 {
					log.Printf("%v: sent: %v, rate: %.2f/sec", a.id, n, float64(n)/time.Now().Sub(start).Seconds())
				}
			} else {
				for i := 0; i < a.ncounters; i++ {
					err := c.Send(ByNumber("counter", i), &DoneMsg{From: a.id})
					if err != nil {
						log.Fatalf("%v: error: %v", a.id, err)
					}
				}
				log.Printf("%v: finished", a.id)
				return true
			}
		}
	}
}
