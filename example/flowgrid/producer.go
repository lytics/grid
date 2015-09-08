package main

import (
	"log"
	"time"

	"github.com/lytics/grid"
	"github.com/lytics/grid/condition"
	"github.com/lytics/grid/ring"
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
	start := time.Now()
	for {
		select {
		case <-exit:
			_, err := s.Store(state)
			if err != nil {
				log.Printf("%v: failed to store state on exit: %v", a.ID(), err)
			}
			return true
		case <-ticker.C:
			_, err = s.Store(state)
			if err != nil {
				log.Fatalf("%v: failed to store state: %v", a.ID(), err)
			}
			if chaos.Roll() {
				return false
			}
		case <-data.Done():
			err := c.Flush()
			if err != nil {
				log.Fatalf("%v: error: %v", a.ID(), err)
			}
			err = c.Send(a.Flow().NewFlowName("leader"), &ResultMsg{Producer: a.ID(), Count: state.SentMessages, From: a.ID(), Duration: time.Now().Sub(start).Seconds()})
			if err != nil {
				log.Printf("%v: error: %v", a.ID(), err)
			} else {
				log.Printf("%v: sent: %v", a.ID(), state.SentMessages)
			}
			_, err = s.Remove()
			if err != nil {
				log.Printf("%v: failed to clean up state: %v", err)
			}
			goto done
		case d := <-data.Next():
			if state.SentMessages == 1 {
				start = time.Now()
			}
			if state.SentMessages%10000 == 0 {
				_, err := s.Store(state)
				if err != nil {
					log.Printf("%v: failed to store state: %v", a.ID(), err)
				}
			}
			err := c.SendBuffered(r.ByInt(state.SentMessages), &DataMsg{Producer: a.ID(), Data: d})
			if err != nil {
				log.Printf("%v: error: %v", a.ID(), err)
			}
			state.SentMessages++
		}
	}

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
