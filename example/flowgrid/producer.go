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

func NewProducerState() *ProducerState {
	return &ProducerState{SentMessages: 0}
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
	state    *ProducerState
	chaos    *Chaos
}

func (a *ProducerActor) ID() string {
	return a.def.ID()
}

func (a *ProducerActor) String() string {
	return a.ID()
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
	a.chaos = NewChaos(a.ID())
	defer a.chaos.Stop()

	d := dfa.New()
	d.SetStartState(Starting)
	d.SetTerminalStates(Exiting, Terminating)

	d.SetTransition(Starting, EverybodyStarted, Running, a.Running)
	d.SetTransition(Starting, GeneralFailure, Exiting, a.Exiting)
	d.SetTransition(Starting, Exit, Exiting, a.Exiting)

	d.SetTransition(Running, SendFailure, Resending, a.Resending)
	d.SetTransition(Running, IndividualFinished, Finishing, a.Finishing)
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

func (a *ProducerActor) Starting() {
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

func (a *ProducerActor) Finishing() {
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

	finished := w.WatchUntil(a.conf.NrConsumers + a.conf.NrConsumers + 1)
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

func (a *ProducerActor) Running() {
	log.Printf("%v: switched to state: %v", a, "Running")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	a.state = NewProducerState()
	s := condition.NewState(a.grid.Etcd(), 30*time.Minute, a.grid.Name(), a.flow.Name(), "state", a.ID())
	if err := s.Init(a.state); err != nil {
		if _, err := s.Fetch(a.state); err != nil {
			a.events <- FetchStateFailure
			return
		}
	}
	log.Printf("%v: starting with state: %v, index: %v", a.ID(), a.state, s.Index())

	// Make some random length string data.
	data := NewDataMaker(a.conf.MsgSize, a.conf.MsgCount-a.state.SentMessages)
	go data.Start(a.exit)

	r := ring.New(a.flow.NewContextualName("consumer"), a.conf.NrConsumers, a.grid)
	for {
		select {
		case <-a.exit:
			if _, err := s.Store(a.state); err != nil {
				log.Printf("%v: failed to save state: %v", a, err)
			}
			a.events <- Exit
			return
		case <-a.chaos.Roll():
			if _, err := s.Store(a.state); err != nil {
				log.Printf("%v: failed to save state: %v", a, err)
			}
			a.events <- GeneralFailure
			return
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				a.events <- GeneralFailure
			}
			if _, err := s.Store(a.state); err != nil {
				a.events <- GeneralFailure
				return
			}
		case <-data.Done():
			if err := a.conn.SendBuffered(a.flow.NewContextualName("leader"), &ResultMsg{Producer: a.ID(), Count: a.state.SentMessages, From: a.ID()}); err != nil {
				a.events <- SendFailure
				return
			}
			if err := a.conn.Flush(); err != nil {
				a.events <- SendFailure
				return
			}
			if _, err := s.Store(a.state); err != nil {
				log.Printf("%v: failed to save state: %v", a, err)
			}
			a.events <- IndividualFinished
			return
		case d := <-data.Next():
			if a.state.SentMessages%10000 == 0 {
				if _, err := s.Store(a.state); err != nil {
					a.events <- GeneralFailure
					return
				}
			}
			if err := a.conn.SendBuffered(r.ByInt(a.state.SentMessages), &DataMsg{Producer: a.ID(), Data: d}); err != nil {
				a.events <- SendFailure
				return
			}
			a.state.SentMessages++
		}
	}
}

func (a *ProducerActor) Resending() {
	log.Printf("%v: switched to state: %v", a, "Resending")
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	fastticker := time.NewTicker(5 * time.Second)
	defer fastticker.Stop()
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
		case <-fastticker.C:
			if err := a.conn.Flush(); err == nil {
				a.events <- SendSuccess
				return
			}
		}
	}
}

func (a *ProducerActor) Exiting() {
	log.Printf("%v: switched to state: %v", a, "Exiting")
	if err := a.conn.Flush(); err != nil {
		log.Printf("%v: failed to flush send buffers, trying again", a)
		if err := a.conn.Flush(); err != nil {
			log.Printf("%v: failed to flush send buffers, data is being dropped", a)
		}
	}
}

func (a *ProducerActor) Terminating() {
	log.Printf("%v: switched to state: %v", a, "Terminating")
}
