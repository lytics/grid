package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/mdmarek/dfa"
)

func NewConsumerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ConsumerActor{
		def:    def,
		conf:   conf,
		flow:   Flow(def.Settings["flow"]),
		events: make(chan dfa.Letter),
		counts: make(map[string]int),
	}
}

type ConsumerActor struct {
	def      *grid.ActorDef
	conf     *Conf
	flow     Flow
	grid     grid.Grid
	conn     grid.Conn
	exit     <-chan bool
	events   chan dfa.Letter
	started  condition.Join
	finished condition.Join
	counts   map[string]int
}

func (a *ConsumerActor) ID() string {
	return a.def.ID()
}

func (a *ConsumerActor) Flow() Flow {
	return a.flow
}

func (a *ConsumerActor) Act(g grid.Grid, exit <-chan bool) bool {
	c, err := grid.NewConn(a.ID(), g.Nats())
	if err != nil {
		log.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer c.Close()
	defer c.Flush()

	a.conn = c
	a.grid = g
	a.exit = exit

	d := dfa.New()
	d.SetStartState(Starting)
	d.SetTerminalStates(Exiting, Terminating)

	d.SetTransition(Starting, EverybodyStarted, Running, a.Running)
	d.SetTransition(Starting, GeneralFailure, Exiting, a.Exiting)
	d.SetTransition(Starting, Exit, Exiting, a.Exiting)

	d.SetTransition(Running, SendFailure, Resending, a.Resending)
	d.SetTransition(Running, Terminal, Finishing, a.Finishing)
	d.SetTransition(Running, GeneralFailure, Exiting, a.Exiting)
	d.SetTransition(Running, Exit, Exiting, a.Exiting)

	d.SetTransition(Resending, SendSuccess, Running, a.Running)
	d.SetTransition(Resending, SendFailure, Resending, a.Resending)
	d.SetTransition(Resending, Exit, Exiting, a.Exiting)

	d.SetTransition(Finishing, EverybodyFinished, Terminating, a.Terminating)
	d.SetTransition(Finishing, GeneralFailure, Exiting, a.Exiting)
	d.SetTransition(Finishing, Exit, Exiting, a.Exiting)

	a.events = make(chan dfa.Letter)
	defer close(a.events)

	err = d.Run(a.events)
	if err != nil {
		log.Fatalf("%v: error: %v", a, err)
	}

	final, err := d.Done()
	if err != nil {
		log.Fatalf("%v: error: %v", a, err)
	}
	if final == Terminating {
		return true
	}
	return false
}

func (a *ConsumerActor) Starting() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 2*time.Minute, a.grid.Name(), a.flow.Name(), "started", a.ID())
	if err := j.Join(); err != nil {
		a.events <- GeneralFailure
		return
	}
	a.started = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.exit, a.grid.Name(), a.flow.Name(), "started")
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				a.events <- GeneralFailure
				return
			}
		case <-w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1):
			a.events <- EverybodyStarted
			return
		}
	}
	return
}

func (a *ConsumerActor) Finishing() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 2*time.Minute, a.grid.Name(), a.flow.Name(), "finished", a.ID())
	if err := j.Join(); err != nil {
		a.events <- GeneralFailure
		return
	}
	a.finished = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.exit, a.grid.Name(), a.flow.Name(), "finished")
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				a.events <- GeneralFailure
				return
			}
			if err := a.finished.Alive(); err != nil {
				a.events <- GeneralFailure
				return
			}
		case <-w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1):
			a.started.Exit()
			a.finished.Exit()
			a.events <- EverybodyFinished
			return
		}
	}
	return
}

func (a *ConsumerActor) Running() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	w := condition.NewCountWatch(a.grid.Etcd(), a.exit, a.grid.Name(), a.flow.Name(), "finished")
	n := 0
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				a.events <- GeneralFailure
				return
			}
		case <-w.WatchUntil(a.conf.NrProducers):
			if err := a.SendCounts(); err != nil {
				a.events <- SendFailure
				return
			} else {
				a.events <- Terminal
				return
			}
		case m := <-a.conn.ReceiveC():
			switch m := m.(type) {
			case DataMsg:
				a.counts[m.Producer]++
				n++
				if n%1000000 == 0 {
					log.Printf("%v: received: %v", a.ID(), n)
				}
				if n%1000 == 0 {
					if err := a.SendCounts(); err != nil {
						a.events <- SendFailure
						return
					}
				}
			}
		}
	}
}

func (a *ConsumerActor) Resending() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				log.Printf("%v: failed to report 'started' liveness, but ignoring to flush send buffers", a)
			}
			if err := a.SendCounts(); err == nil {
				a.events <- SendSuccess
				return
			}
		}
	}
}

func (a *ConsumerActor) Exiting() {

}

func (a *ConsumerActor) Terminating() {

}

func (a *ConsumerActor) SendCounts() error {
	for p, n := range a.counts {
		if err := a.conn.Send(a.flow.NewContextualName("leader"), &ResultMsg{Producer: p, Count: n, From: a.ID()}); err != nil {
			return err
		} else {
			delete(a.counts, p)
		}
	}
	return nil
}
