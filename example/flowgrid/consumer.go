package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
	"github.com/mdmarek/dfa"
)

func NewConsumerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ConsumerActor{
		def:    def,
		conf:   conf,
		flow:   Flow(def.Settings["flow"]),
		events: make(chan dfa.Letter),
		state:  NewConsumerState(),
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
	state    *ConsumerState
	chaos    *Chaos
}

func (a *ConsumerActor) ID() string {
	return a.def.ID()
}

func (a *ConsumerActor) String() string {
	return a.ID()
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
	a.chaos = NewChaos(a.ID())
	defer a.chaos.Stop()

	d := dfa.New()
	d.SetStartState(Starting)
	d.SetTerminalStates(Exiting, Terminating)

	d.SetTransition(Starting, EverybodyStarted, Running, a.Running)
	d.SetTransition(Starting, GeneralFailure, Exiting, a.Exiting)
	d.SetTransition(Starting, Exit, Exiting, a.Exiting)

	d.SetTransition(Running, SendFailure, Resending, a.Resending)
	d.SetTransition(Running, EverybodyFinished, Finishing, a.Finishing)
	d.SetTransition(Running, GeneralFailure, Exiting, a.Exiting)
	d.SetTransition(Running, Exit, Exiting, a.Exiting)

	d.SetTransition(Resending, SendSuccess, Running, a.Running)
	d.SetTransition(Resending, SendFailure, Resending, a.Resending)
	d.SetTransition(Resending, GeneralFailure, Exiting, a.Exiting)
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
	go a.Starting()

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
	log.Printf("%v: switched to state: %v", a, "Starting")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 2*time.Minute, a.grid.Name(), a.flow.Name(), "started", a.ID())
	if err := j.Rejoin(); err != nil {
		a.events <- GeneralFailure
		return
	}
	a.started = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "started")
	defer w.Stop()

	started := w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1)
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-a.chaos.Roll():
			a.events <- GeneralFailure
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				a.events <- GeneralFailure
				return
			}
		case <-started:
			a.events <- EverybodyStarted
			return
		}
	}
	return
}

func (a *ConsumerActor) Finishing() {
	log.Printf("%v: switched to state: %v", a, "Finishing")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 10*time.Minute, a.grid.Name(), a.flow.Name(), "finished", a.ID())
	if err := j.Rejoin(); err != nil {
		a.events <- GeneralFailure
		return
	}
	a.finished = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer w.Stop()

	finished := w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1)
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-a.chaos.Roll():
			a.events <- GeneralFailure
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
		case <-finished:
			a.started.Exit()
			a.finished.Alive()
			a.events <- EverybodyFinished
			return
		case err := <-w.WatchError():
			log.Printf("%v: error: %v", a, err)
			a.events <- GeneralFailure
			return
		}
	}
	return
}

func (a *ConsumerActor) Running() {
	log.Printf("%v: switched to state: %v", a, "Running")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	w := condition.NewNameWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer w.Stop()

	n := 0
	finished := w.WatchUntil(ring.New(a.flow.NewContextualName("producer"), a.conf.NrProducers, a.grid))
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-a.chaos.Roll():
			a.events <- GeneralFailure
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				a.events <- GeneralFailure
				return
			}
		case <-finished:
			if err := a.SendCounts(); err != nil {
				a.events <- SendFailure
				return
			} else {
				a.events <- EverybodyFinished
				return
			}
		case err := <-w.WatchError():
			log.Printf("%v: error: %v", a, err)
			a.events <- GeneralFailure
			return
		case m := <-a.conn.ReceiveC():
			switch m := m.(type) {
			case DataMsg:
				a.state.Counts[m.Producer]++
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
	log.Printf("%v: switched to state: %v", a, "Resending")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.exit:
			a.events <- Exit
			return
		case <-a.chaos.Roll():
			a.events <- GeneralFailure
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
	log.Printf("%v: switched to state: %v", a, "Exiting")
	if err := a.conn.Flush(); err != nil {
		log.Printf("%v: failed to flush send buffers, trying again", a)
		if err := a.conn.Flush(); err != nil {
			log.Printf("%v: failed to flush send buffers, data is being dropped", a)
		}
	}
}

func (a *ConsumerActor) Terminating() {
	log.Printf("%v: switched to state: %v", a, "Terminating")
}

func (a *ConsumerActor) SendCounts() error {
	for p, n := range a.state.Counts {
		if err := a.conn.Send(a.flow.NewContextualName("leader"), &ResultMsg{Producer: p, Count: n, From: a.ID()}); err != nil {
			return err
		} else {
			delete(a.state.Counts, p)
		}
	}
	return nil
}

type ConsumerState struct {
	Counts map[string]int
}

func NewConsumerState() *ConsumerState {
	return &ConsumerState{Counts: make(map[string]int)}
}
