package main

import (
	"encoding/gob"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lytics/grid/grid2"
)

const (
	Leader   = "leader"
	Follower = "follower"
)

func main() {
	g := grid2.New("firstgrid", []string{"http://localhost:2379"}, []string{"nats://localhost:4222"}, &maker{})

	// Start the grid2.
	exit, err := g.Start()
	if err != nil {
		log.Fatalf("error: failed to start grid: %v", err)
	}

	// Start the leader actor.
	err = g.StartActor(grid2.NewActorDef(Leader))
	if err != nil {
		log.Fatalf("error: failed to start actor: %v", err)
	}
	// Start the follower actor.
	err = g.StartActor(grid2.NewActorDef(Follower))
	if err != nil {
		log.Fatalf("error: failed to start actor: %v", err)
	}

	// Wait for a signal from the user to exit, or
	// exit if exit channel is closed by grid2.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		select {
		case <-sig:
			log.Printf("shutting down")
			g.Stop()
		case <-exit:
		}
	}()

	<-exit
	log.Printf("shutdown complete")
}

type maker struct{}

// MakeActor, maps names to actor types and returns instances
// of those types.
func (m *maker) MakeActor(def *grid2.ActorDef) (grid2.Actor, error) {
	return &actor{def: def}, nil
}

type actor struct {
	def *grid2.ActorDef
}

// ID, must match the name passed to MakeActor.
func (a *actor) ID() string {
	return a.def.ID()
}

// Act, returns a value of true to signal that it is
// done and should not be scheduled again.
func (a *actor) Act(g grid2.Grid, exit <-chan bool) bool {
	tx, err := grid2.NewSender(g.Nats(), 100)
	if err != nil {
		log.Printf("%v: error: %v", a.ID(), err)
	}
	defer tx.Close()

	rx, err := grid2.NewReceiver(g.Nats(), a.ID(), 1, 0)
	if err != nil {
		log.Printf("%v: error: %v", a.ID(), err)
	}
	defer rx.Close()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-exit:
			return true
		case now := <-ticker.C:
			if a.ID() == Leader {
				tx.Send(Follower, &Msg{Time: now, From: a.ID()})
			}
		case m := <-rx.Msgs():
			log.Printf("%v: received: %v", a.ID(), m)
		}
	}
}

type Msg struct {
	Time time.Time
	From string
}

func init() {
	gob.Register(Msg{})
}
