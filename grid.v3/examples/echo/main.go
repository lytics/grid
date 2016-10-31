package main

import (
	"context"
	"errors"
	"time"

	"fmt"
	"os"

	"flag"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3"
	"github.com/lytics/grid/grid.v3/discovery"
	"github.com/lytics/grid/grid.v3/message"
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
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	sub, err := mm.Subscribe(timeout, id, mailboxSize)
	cancel()
	successOrDie(err)
	defer sub.Unsubscribe(c)

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
	address := flag.String("address", "", "bind address for gRPC listener")
	flag.Parse()

	etcd, err := etcdv3.New(etcdv3.Config{
		Endpoints: []string{"localhost:2379"},
	})
	successOrDie(err)
	defer etcd.Close()

	dc, err := discovery.New(*address, etcd)
	successOrDie(err)
	defer dc.Stop()

	err = dc.Start()
	successOrDie(err)

	mm, err := message.New(dc)
	successOrDie(err)
	defer mm.Stop()

	echo := Echo("echo")
	err = grid.Register(dc, mm, echo)
	successOrDie(err)

	go func() {
		existing := make(map[string]bool)
		for {
			time.Sleep(2 * time.Second)
			timeout, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			peers, err := grid.Peers(timeout, echo.Namespace())
			cancel()
			successOrDie(err)
			for _, peer := range peers {
				if !existing[peer] {
					existing[peer] = true
					def := grid.NewActorDef(echo.Namespace(), "echo-actor")
					timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					err = grid.RequestActorStart(timeout, peer, def)
					cancel()
					if err != nil {
						fmt.Printf("actor start request failed for peer: %v, error: %v\n", peer, err)
					}
				}
			}
		}
	}()

	err = mm.Serve()
	successOrDie(err)
}

func successOrDie(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v", err)
		os.Exit(1)
	}
}
