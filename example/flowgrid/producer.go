package main

import (
	"log"
	"time"

	"github.com/lytics/dfa"
	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
)

type ProducerState struct {
	SentMessages int `json:"sent_messages"`
}

func NewProducerState() *ProducerState {
	return &ProducerState{SentMessages: 0}
}

func NewProducerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ProducerActor{
		def:  def,
		conf: conf,
		flow: Flow(def.Settings["flow"]),
	}
}

type ProducerActor struct {
	def      *grid.ActorDef
	conf     *Conf
	flow     Flow
	grid     grid.Grid
	conn     grid.Conn
	exit     <-chan bool
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
	d.SetTransitionLogger(func(state dfa.State) {
		log.Printf("%v: switched to state: %v", a, state)
	})

	d.SetTransition(Starting, EverybodyStarted, Running, a.Running)
	d.SetTransition(Starting, Failure, Exiting, a.Exiting)
	d.SetTransition(Starting, Exit, Exiting, a.Exiting)

	d.SetTransition(Running, SendFailure, Resending, a.Resending)
	d.SetTransition(Running, IndividualFinished, Finishing, a.Finishing)
	d.SetTransition(Running, Failure, Exiting, a.Exiting)
	d.SetTransition(Running, Exit, Exiting, a.Exiting)

	d.SetTransition(Resending, SendSuccess, Running, a.Running)
	d.SetTransition(Resending, SendFailure, Resending, a.Resending)
	d.SetTransition(Resending, Failure, Exiting, a.Exiting)
	d.SetTransition(Resending, Exit, Exiting, a.Exiting)

	d.SetTransition(Finishing, EverybodyFinished, Terminating, a.Terminating)
	d.SetTransition(Finishing, Failure, Exiting, a.Exiting)
	d.SetTransition(Finishing, Exit, Exiting, a.Exiting)

	err = d.Run(a.Starting)
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

func (a *ProducerActor) Starting() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 2*time.Minute, a.grid.Name(), a.flow.Name(), "started", a.ID())
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	a.started = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "started")
	defer w.Stop()

	started := w.WatchUntil(a.conf.NrConsumers + a.conf.NrProducers + 1)
	for {
		select {
		case <-a.exit:
			return Exit
		case <-a.chaos.C:
			return Failure
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				return Failure
			}
		case <-started:
			return EverybodyStarted
		}
	}
}

func (a *ProducerActor) Finishing() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	j := condition.NewJoin(a.grid.Etcd(), 10*time.Minute, a.grid.Name(), a.flow.Name(), "finished", a.ID())
	if err := j.Rejoin(); err != nil {
		return Failure
	}
	a.finished = j

	w := condition.NewCountWatch(a.grid.Etcd(), a.grid.Name(), a.flow.Name(), "finished")
	defer w.Stop()

	finished := w.WatchUntil(a.conf.NrConsumers + a.conf.NrConsumers + 1)
	for {
		select {
		case <-a.exit:
			return Exit
		case <-a.chaos.C:
			return Failure
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				return Failure
			}
			if err := a.finished.Alive(); err != nil {
				return Failure
			}
		case <-finished:
			a.started.Exit()
			a.finished.Alive()
			return EverybodyFinished
		case err := <-w.WatchError():
			log.Printf("%v: error: %v", a, err)
			return Failure
		}
	}
}

func (a *ProducerActor) Running() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	a.state = NewProducerState()
	s := condition.NewState(a.grid.Etcd(), 30*time.Minute, a.grid.Name(), a.flow.Name(), "state", a.ID())
	if err := s.Init(a.state); err != nil {
		if _, err := s.Fetch(a.state); err != nil {
			return FetchStateFailure
		}
	}
	log.Printf("%v: running with state: %v, index: %v", a.ID(), a.state, s.Index())

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
			return Exit
		case <-a.chaos.C:
			if _, err := s.Store(a.state); err != nil {
				log.Printf("%v: failed to save state: %v", a, err)
			}
			return Failure
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				return Failure
			}
			if _, err := s.Store(a.state); err != nil {
				return Failure
			}
		case <-data.Done():
			if err := a.conn.SendBuffered(a.flow.NewContextualName("leader"), &ResultMsg{Producer: a.ID(), Count: a.state.SentMessages, From: a.ID()}); err != nil {
				return SendFailure
			}
			if err := a.conn.Flush(); err != nil {
				return SendFailure
			}
			if _, err := s.Store(a.state); err != nil {
				log.Printf("%v: failed to save state: %v", a, err)
			}
			return IndividualFinished
		case d := <-data.Next():
			if a.state.SentMessages%10000 == 0 {
				if _, err := s.Store(a.state); err != nil {
					log.Printf("%v: failed to save state: %v", a, err)
					return Failure
				}
			}
			if err := a.conn.SendBuffered(r.ByInt(a.state.SentMessages), &DataMsg{Producer: a.ID(), Data: d}); err != nil {
				if _, err := s.Store(a.state); err != nil {
					log.Printf("%v: failed to save state: %v", a, err)
				}
				return SendFailure
			}
			a.state.SentMessages++
		}
	}
}

func (a *ProducerActor) Resending() dfa.Letter {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	fastticker := time.NewTicker(5 * time.Second)
	defer fastticker.Stop()
	for {
		select {
		case <-a.exit:
			return Exit
		case <-a.chaos.C:
			return Failure
		case <-ticker.C:
			if err := a.started.Alive(); err != nil {
				log.Printf("%v: failed to report 'started' liveness, but ignoring to flush send buffers", a)
			}
		case <-fastticker.C:
			if err := a.conn.Flush(); err == nil {
				return SendSuccess
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
}
