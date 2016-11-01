package main

import (
	"context"
	"errors"
	"time"

	"fmt"
	"os"

	"flag"

	"github.com/lytics/grid/grid.v3"
)

const mailboxSize = 10

type StartMsg string

type StopMsg struct{}

type EchoMsg string

type EchoActor struct{}

func (a *EchoActor) Act(c context.Context) {
	id, err := grid.ContextActorID(c)
	successOrDie(err)

	mm, err := grid.ContextMessenger(c)
	successOrDie(err)

	// Subscribe to message send to the actor's ID.
	sub, err := mm.Subscribe(id, mailboxSize)
	successOrDie(err)
	defer sub.Unsubscribe()

	// Listen and respond to requests.
	fmt.Println("hello")
	for {
		select {
		case e := <-sub.Mailbox():
			switch msg := e.Msg.(type) {
			case StopMsg:
				e.Ack()
				return
			case EchoMsg:
				e.Respond(msg)
			}
		}
	}
}

// Echo grid, anything that can make actors is a node in the grid.
type Echo string

// Namespace of this echo grid.
func (e Echo) Namespace() string {
	return string(e)
}

// MakeActor for the grid.
func (e Echo) MakeActor(def *grid.ActorDef) (grid.Actor, error) {
	switch def.Type {
	case "echo-actor":
		return &EchoActor{}, nil
	default:
		return nil, errors.New("unknown actor type")
	}
}

func main() {
	address := flag.String("address", "", "bind address for gRPC")
	flag.Parse()

	echo := Echo("echo")
	go detectNewPeersAndStartActors(echo)

	err := grid.Serve(*address, []string{"localhost:2379"}, echo)
	successOrDie(err)
}

func successOrDie(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
		os.Exit(1)
	}
}

func detectNewPeersAndStartActors(g grid.Grid) {
	existing := make(map[string]bool)
	for {
		time.Sleep(2 * time.Second)

		// Ask for current peers.
		timeout, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		peers, err := grid.Peers(timeout, g.Namespace())
		cancel()
		successOrDie(err)

		// Loop over result and check is any peers are new.
		for _, peer := range peers {
			if existing[peer] {
				continue
			}

			// Define an actor.
			existing[peer] = true
			def := grid.NewActorDef(g.Namespace(), "echo-actor")

			// On new peer, start an actor.
			timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = grid.RequestActorStart(timeout, peer, def)
			cancel()
			if err != nil {
				fmt.Printf("actor start request failed for peer: %v, error: %v\n", peer, err)
			}
		}
	}
}
