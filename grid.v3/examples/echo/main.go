package main

import (
	"context"
	"errors"
	"time"

	"fmt"
	"os"

	"flag"

	"net"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3"
)

const mailboxSize = 10

type StartMsg string

type StopMsg struct{}

type EchoMsg string

type EchoActor struct{}

func (a *EchoActor) Act(ctx context.Context) {
	id, err := grid.ContextActorID(ctx)
	successOrDie(err)

	// Subscribe to message sent to the actor's ID.
	mailbox, err := grid.NewMailbox(ctx, id, mailboxSize)
	successOrDie(err)
	defer mailbox.Close()

	// Listen and respond to requests.
	fmt.Println("hello")
	for {
		select {
		case e := <-mailbox.C:
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
type Echo struct{}

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

	etcd, err := etcdv3.New(etcdv3.Config{Endpoints: []string{"localhost:2379"}})
	successOrDie(err)

	echo := Echo{}
	go detectNewPeersAndStartActors("echo", etcd)

	g, err := grid.NewServer("echo", etcd, echo)
	successOrDie(err)

	lis, err := net.Listen("tcp", *address)
	successOrDie(err)

	err = g.Serve(lis)
	successOrDie(err)
}

func successOrDie(err error, context ...string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v, context: %v\n", err, context)
		os.Exit(1)
	}
}

func detectNewPeersAndStartActors(namespace string, etcd *etcdv3.Client) {
	client, err := grid.NewClient(namespace, etcd)
	successOrDie(err)

	existing := make(map[string]bool)
	for {
		time.Sleep(2 * time.Second)

		// Ask for current peers.
		peers, err := client.Peers(2 * time.Second)
		successOrDie(err)

		// Loop over result and check is any peers are new.
		for _, peer := range peers {
			if existing[peer] {
				continue
			}

			// Define an actor.
			existing[peer] = true
			def := grid.NewActorDef("echo-actor")

			// On new peer, start an actor.
			_, err := client.Request(2*time.Second, peer, def)
			if err != nil {
				fmt.Printf("actor start request failed for peer: %v, error: %v\n", peer, err)
			}
		}
	}
}
