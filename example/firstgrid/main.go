package main

import (
	"encoding/gob"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lytics/grid"
)

const (
	Leader   = "leader"
	Follower = "follower"
)

func main() {
	// Use the hostname as the node identifier.
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("error: failed to discover hostname")
	}

	g := grid.New("firstgrid", hostname, []string{"http://localhost:2379"}, []string{"nats://localhost:4222"}, &maker{})

	// Start the grid.
	exit, err := g.Start()
	if err != nil {
		log.Fatalf("error: failed to start grid: %v", err)
	}

	// Start the leader actor.
	err = g.StartActor(grid.NewActorDef(Leader))
	if err != nil {
		log.Fatalf("error: failed to start actor: %v", err)
	}
	// Start the follower actor.
	err = g.StartActor(grid.NewActorDef(Follower))
	if err != nil {
		log.Fatalf("error: failed to start actor: %v", err)
	}

	// Wait for a signal from the user to exit, or
	// exit if exit channel is closed by grid.
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
func (m *maker) MakeActor(def *grid.ActorDef) (grid.Actor, error) {
	return &actor{def: def}, nil
}

type actor struct {
	def *grid.ActorDef
}

// ID, must match the name passed to MakeActor.
func (a *actor) ID() string {
	return a.def.ID()
}

// Act, returns a value of true to signal that it is
// done and should not be scheduled again.
func (a *actor) Act(g grid.Grid, exit <-chan bool) bool {
	tx, err := grid.NewSender(g.Nats(), 100)
	if err != nil {
		log.Printf("%v: error: %v", a.ID(), err)
	}
	defer tx.Close()

	rx, err := grid.NewReceiver(g.Nats(), a.ID(), 1, 0)
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
