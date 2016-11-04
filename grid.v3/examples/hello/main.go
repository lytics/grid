package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3"
)

const mailboxSize = 10

type HelloMsg struct{}

type Leader struct{}

func (a *Leader) Act(c context.Context) {
	client, err := grid.ContextClient(c)
	successOrDie(err)

	existing := make(map[string]bool)
	for {
		time.Sleep(2 * time.Second)

		// Ask for current peers.
		peers, err := client.Peers(2 * time.Second)
		successOrDie(err)

		// Loop over result and check if any peers are new.
		for _, peer := range peers {
			if existing[peer] {
				continue
			}

			// Define an actor.
			existing[peer] = true
			def := grid.NewActorDef("worker-%d", len(existing))
			def.Type = "worker"

			// On new peers start a worker.
			_, err := client.Request(2*time.Second, peer, def)
			successOrDie(err)
		}
	}
}

type Worker struct{}

func (a *Worker) Act(ctx context.Context) {
	fmt.Println("hello")

	id, err := grid.ContextActorID(ctx)
	successOrDie(err)

	// Subscribe to messages sent to the actor's ID.
	mailbox, err := grid.NewMailbox(ctx, id, mailboxSize)
	successOrDie(err)
	defer mailbox.Close()

	// Listen and respond to requests.
	for {
		select {
		case <-ctx.Done():
			fmt.Println("goodbye...")
			return
		case e := <-mailbox.C:
			switch e.Msg.(type) {
			case HelloMsg:
				e.Ack()
				fmt.Println("hi from", id)
			}
		}
	}
}

// Hello grid.
type Hello struct{}

// MakeActor given the definition.
func (e Hello) MakeActor(def *grid.ActorDef) (grid.Actor, error) {
	switch def.Type {
	case "leader":
		return &Leader{}, nil
	case "worker":
		return &Worker{}, nil
	default:
		return nil, errors.New("unknown actor type")
	}
}

func main() {
	address := flag.String("address", "", "bind address for gRPC")
	flag.Parse()

	etcd, err := etcdv3.New(etcdv3.Config{Endpoints: []string{"localhost:2379"}})
	successOrDie(err)

	g, err := grid.NewServer(etcd, "hello", Hello{})
	successOrDie(err)

	// Check for exit signals.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		fmt.Println("shutting down...")
		g.Stop()
		fmt.Println("shutdown complete")
	}()

	lis, err := net.Listen("tcp", *address)
	successOrDie(err)

	err = g.Serve(lis)
	successOrDie(err)
}

func successOrDie(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
