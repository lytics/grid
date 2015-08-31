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
	g := grid.New("firstgrid", []string{"http://localhost:2379"}, []string{"nats://localhost:4222"}, &maker{})

	// Start the grid.
	exit, err := g.Start()
	if err != nil {
		log.Fatalf("error: failed to start grid: %v", err)
	}

	// Start the leader actor.
	err = g.StartActor(Leader)
	if err != nil {
		log.Fatalf("error: failed to start actor: %v", err)
	}
	// Start the follower actor.
	err = g.StartActor(Follower)
	if err != nil {
		log.Fatalf("error: failed to start actor: %v", err)
	}

	// Wait for a signal from the user to exit, or
	// exit if exit channel is closed by grid.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sig:
		log.Printf("shutting down")
		g.Stop()
	case <-exit:
		log.Printf("shutting down, grid exited")
	}
}

type maker struct{}

// MakeActor, maps names to actor types and returns instances
// of those types.
func (m *maker) MakeActor(name string) (grid.Actor, error) {
	return &actor{id: name}, nil
}

type actor struct {
	id string
}

// ID, must match the name passed to MakeActor.
func (a *actor) ID() string {
	return a.id
}

// Act, returns a value of true to signal that it is
// done and should not be scheduled again.
func (a *actor) Act(g grid.Grid, exit <-chan bool) bool {
	c, err := grid.NewConn(a.id, g.Nats())
	if err != nil {
		log.Printf("%v: error: %v", a.id, err)
	}
	defer c.Close()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-exit:
			return true
		case now := <-ticker.C:
			if a.ID() == Leader {
				c.Send(Follower, &Msg{Time: now, From: a.id})
			}
		case m := <-c.ReceiveC():
			log.Printf("%v: received: %v", a.id, m)
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
