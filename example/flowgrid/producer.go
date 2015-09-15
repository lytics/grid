package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
	"github.com/mdmarek/dfa"
)

type ProducerState struct {
	SentMessages int `json:"sent_messages"`
}

func NewProducerState(count int) *ProducerState {
	return &ProducerState{SentMessages: count}
}

func NewProducerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ProducerActor{
		def:    def,
		conf:   conf,
		flow:   Flow(def.Settings["flow"]),
		events: make(chan dfa.Letter),
	}
}

type ProducerActor struct {
	def      *grid.ActorDef
	conf     *Conf
	flow     Flow
	grid     grid.Grid
	conn     grid.Conn
	exit     <-chan bool
	events   chan dfa.Letter
	started  condition.Join
	finished condition.Join
}

func (a *ProducerActor) ID() string {
	return a.def.ID()
}

func (a *ProducerActor) Act(g grid.Grid, exit <-chan bool) bool {
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

func (a *ProducerActor) Starting() {
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

func (a *ProducerActor) Finishing() {
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

func (a *ProducerActor) Running() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	state := NewProducerState(0)
	s := condition.NewState(a.grid.Etcd(), 10*time.Minute, a.grid.Name(), a.flow.Name(), "state", a.ID())
	if err := s.Init(state); err != nil {
		if _, err := s.Fetch(state); err != nil {
			a.events <- FetchStateFailure
			return
		}
	}
	log.Printf("%v: starting with state: %v, index: %v", a.ID(), state.SentMessages, s.Index())

	// Make some random length string data.
	data := NewDataMaker(a.conf.MsgSize, a.conf.MsgCount-state.SentMessages)
	go data.Start(a.exit)

	r := ring.New(a.flow.NewContextualName("consumer"), a.conf.NrConsumers, a.grid)
	chaos := NewChaos()
	for {
		select {
		case <-a.exit:
			if _, err := s.Store(state); err != nil {
				log.Printf("%v: failed to save state when exit requested", a)
			}
			a.events <- Exit
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				a.events <- GeneralFailure
			}
			if _, err := s.Store(state); err != nil {
				a.events <- GeneralFailure
				return
			}
			if chaos.Roll() {
				a.events <- Exit
				return
			}
		case <-data.Done():
			if err := a.conn.SendBuffered(a.flow.NewContextualName("leader"), &ResultMsg{Producer: a.ID(), Count: state.SentMessages, From: a.ID()}); err != nil {
				a.events <- SendFailure
				return
			}
			if err := a.conn.Flush(); err != nil {
				a.events <- SendFailure
				return
			}
			if _, err := s.Remove(); err != nil {
				log.Printf("%v: failed to clean up state: %v", err)
			}
			a.events <- Terminal
			return
		case d := <-data.Next():
			if state.SentMessages%10000 == 0 {
				if _, err := s.Store(state); err != nil {
					a.events <- GeneralFailure
					return
				}
			}
			if err := a.conn.SendBuffered(r.ByInt(state.SentMessages), &DataMsg{Producer: a.ID(), Data: d}); err != nil {
				a.events <- SendFailure
				return
			}
			state.SentMessages++
		}
	}
}

func (a *ProducerActor) Resending() {
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
			if err := a.conn.Flush(); err == nil {
				a.events <- SendSuccess
				return
			}
		}
	}
}

func (a *ProducerActor) Exiting() {
	if err := a.conn.Flush(); err != nil {
		log.Printf("%v: failed to flush send buffers, trying again", a)
		if err := a.conn.Flush(); err != nil {
			log.Printf("%v: failed to flush send buffers, data is being dropped", a)
		}
	}
}

func (a *ProducerActor) Terminating() {
	log.Printf("%v: terminating", a)
}
