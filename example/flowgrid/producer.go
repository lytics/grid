package main

import (
	"container/ring"
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/mdmarek/dfa"
)

var (
	// States
	Registering = dfa.State("registering")
	Exiting     = dfa.State("exiting")
	// Letters
	RegisterSuccess   = dfa.Letter("register-success")
	RegisterFailure   = dfa.Letter("register-failure")
	SendFailure       = dfa.Letter("send-failure")
	SendSuccess       = dfs.Letter("send-success")
	StoreStateFailure = dfa.Letter("store-state-failure")
	EverybodyReady    = dfa.Letter("everybody-ready")
	ExitWanted        = dfa.Letter("exit-wanted")
	Terminal          = dfa.Letter("terminal")
)

func NewProducerActor(def *grid.ActorDef, conf *Conf) grid.Actor {
	return &ProducerActor{def: def, conf: conf}
}

type ProducerActor struct {
	def  *grid.ActorDef
	conf *Conf
}

func (a *ProducerActor) ID() string {
	return a.def.ID()
}

func (a *ProducerActor) Flow() Flow {
	return Flow(a.def.Settings["flow"])
}

func (a *ProducerActor) Act(g grid.Grid, exit <-chan bool) bool {
	c, err := grid.NewConn(a.ID(), g.Nats())
	if err != nil {
		log.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer c.Close()
	defer c.Flush()

	p := dfa.New()
	p.SetStartState(Registering)
	p.SetTerminalStates(Exiting)

	p.SetTransition(Registering, RegisterSuccess, ExitWanted, a)
	p.SetTransition(Registering, RegisterFailure, ExitWanted, a)

	in := make(chan dfa.Letter)
	err = p.Run(in)
	if err != nil {
		log.Fatalf("%v: error: %v", a, err)
	}

	if _, err := p.Done(); err != nil {
		log.Fatalf("%v: error: %v", a, err)
	} else {
		return true
	}
}

func (a *ProducerActor) Transition(from dfa.State, l dfa.Letter, to dfa.State) {
	log.Println("%v: %v + %v -> %v", a, from, l, to)
}

func (a *ProducerActor) Act2(g grid.Grid, exit <-chan bool) <-chan dfa.Letter {
	c, err := grid.NewConn(a.ID(), g.Nats())
	if err != nil {
		log.Fatalf("%v: error: %v", a.ID(), err)
	}
	defer c.Close()
	defer c.Flush()

done:
	j := condition.NewJoin(g.Etcd(), 5*time.Minute, g.Name(), a.Flow().Name(), "producers", "done", a.ID())
	err = j.Join()
	if err != nil {
		log.Printf("%v: failed to register: %v", a.ID(), err)
		return false
	}
	defer j.Exit()
	for {
		select {
		case <-exit:
			return true
		case <-ticker.C:
			err := j.Alive()
			if err != nil {
				log.Printf("%v: failed to report liveness: %v", a.ID(), err)
				return false
			}
			if chaos.Roll() {
				return false
			}
		}
	}
}

type ProducerState struct {
	SentMessages int `json:"sent_messages"`
}

func NewProducerState(count int) *ProducerState {
	return &ProducerState{SentMessages: count}
}

type Producer struct {
	j      condition.Join
	events chan dfa.Letter
	exit   <-chan bool
	conf   *Conf
	grid.Grid
	grid.Conn
}

func (p *Producer) Registering() {
	p.reg = condition.NewJoin(p.Etcd(), 2*time.Minute, p.Name(), "producer", p.ID())
	if err := p.j.Join(); err != nil {
		p.events <- RegisterFailure
	} else {
		p.events <- RegisterSuccess
	}
	return
}

func (p *Producer) WaitingStart() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	w := condition.Watch(p.Etcd(), exit, g.Name(), "ready")
	for {
		select {
		case <-ticker.C:
			if err := p.j.Alive(); err != nil {
				p.events <- RegisterFailure
				return
			}
		case <-w.WatchUntil(p.conf.NrConsumers + p.conf.NrProducers + 1):
			p.events <- EverybodyReady
			return
		}
	}
	return
}

func (p *Producer) WaitingFinish() {

}

func (p *Producer) Running() {
	state := NewProducerState(0)
	s := condition.NewState(g.Etcd(), 5*time.Minute, g.Name(), "state", a.Flow().Name(), a.ID())
	err = s.Init(state)
	if err != nil {
		_, err := s.Fetch(state)
		if err != nil {
			log.Fatalf("%v: failed to init or fetch state: %v", a.ID(), err)
		}
	}
	log.Printf("%v: starting with state: %v, index: %v", a.ID(), state.SentMessages, s.Index())

	// Make some random length string data.
	data := NewDataMaker(a.conf.MsgSize, a.conf.MsgCount-state.SentMessages)
	go data.Start(exit)

	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	r := ring.New(a.Flow().NewFlowName("consumer"), a.conf.NrConsumers, g)
	chaos := NewChaos()
	for {
		select {
		case <-exit:
			p.events <- ExitWanted
			return
		case <-ticker.C:
			if err := p.j.Alive(); err != nil {
				p.events <- RegisterFailure
			}
			if _, err = s.Store(state); err != nil {
				p.events <- StoreStateFailure
				return
			}
			if chaos.Roll() {
				p.events <- ExitWanted
				return
			}
		case <-data.Done():
			if err := c.SendBuffered(a.Flow().NewFlowName("leader"), &ResultMsg{Producer: a.ID(), Count: state.SentMessages, From: a.ID()}); err != nil {
				p.events <- SendFailure
				return
			}
			if err := c.Flush(); err != nil {
				p.events <- SendFailure
				return
			}
			_, err = s.Remove()
			if err != nil {
				log.Printf("%v: failed to clean up state: %v", err)
			}
			p.events <- Terminal
			return
		case d := <-data.Next():
			if state.SentMessages%10000 == 0 {
				if _, err := s.Store(state); err != nil {
					p.events <- StoreStateFailure
					return
				}
			}
			if err := c.SendBuffered(r.ByInt(state.SentMessages), &DataMsg{Producer: a.ID(), Data: d}); err != nil {
				p.events <- SendFailure
				return
			}
			state.SentMessages++
		}
	}
}

func (p *Producer) Resending() {
	fasttick := time.NewTicker(2 * time.Second)
	defer fasttick.Stop()
	slowtick := time.NewTicker(30 * time.Second)
	defer slowtick.Stop()
	for {
		select {
		case <-p.exit:
			p.events <- ExitWanted
			return
		case <-fasttick.C:
			if err := p.Flush(); err == nil {
				p.events <- SendSuccess
				return
			}
		case <-slowtick.C:
			if err := p.Flush(); err != nil {
				p.events <- SendFailure
				return
			} else {
				p.events <- SendSuccess
				return
			}
		}
	}
}
