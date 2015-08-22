package main

import (
	"log"
	"strconv"
	"strings"

	"github.com/lytics/grid"
)

func NewCounterActor(id string, nmessages, ncounters int) grid.Actor {
	parts := strings.Split(id, ".")
	nr, err := strconv.Atoi(parts[2])
	if err != nil {
		log.Printf("%v: fatal: %v", id, err)
		return nil
	}
	return &CounterActor{id: id, nr: nr, nmessages: nmessages, ncounters: ncounters}
}

type CounterActor struct {
	id        string
	nr        int
	nmessages int
	ncounters int
}

func (a *CounterActor) ID() string {
	return a.id
}

func (a *CounterActor) Act(g grid.Grid, exit <-chan bool) bool {
	c := grid.NewConn(a.id, g.Nats())
	counts := make(map[string]map[int]bool)
	for {
		select {
		case <-exit:
			return true
		case m := <-c.ReceiveC():
			switch m := m.(type) {
			case CntMsg:
				bucket, ok := counts[m.From]
				if !ok {
					bucket = make(map[int]bool)
					counts[m.From] = bucket
				}
				bucket[m.Number] = true
			case DoneMsg:
				bucket := counts[m.From]
				missing := 0
				for i := 0; i < a.nmessages; i++ {
					if i%a.ncounters == a.nr {
						if !bucket[i] {
							missing++
						}
					}
				}
				log.Printf("%v: from actor: %v, missing: %v", a.id, m.From, missing)
			}
		}
	}
}
