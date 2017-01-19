package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3"
)

const timeout = 2 * time.Second

type LeaderActor struct {
	client *grid.Client
}

func (a *LeaderActor) Act(c context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	existing := make(map[string]bool)
	for {
		select {
		case <-c.Done():
			return
		case <-ticker.C:
			// Ask for current peers.
			peers, err := a.client.Query(timeout, grid.Peers)
			successOrDie(err)

			// Check for new peers.
			for _, peer := range peers {
				if existing[peer] {
					continue
				}

				// Define a worker.
				existing[peer] = true
				def := grid.NewActorDef("worker-%d", len(existing))
				def.Type = "worker"

				// On new peers start the worker.
				_, err := a.client.Request(timeout, peer, def)
				successOrDie(err)
			}
		}
	}
}

type WorkerActor struct{}

func (a *WorkerActor) Act(ctx context.Context) {
	fmt.Println("hello world")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("goodbye...")
			return
		}
	}
}

// HelloGrid is a grid, because it has the MakeActor method.
type HelloGrid struct {
	client *grid.Client
}

// MakeActor given the definition of the actor.
func (hg HelloGrid) MakeActor(def *grid.ActorDef) (grid.Actor, error) {
	switch def.Type {
	case "leader":
		return &LeaderActor{client: hg.client}, nil
	case "worker":
		return &WorkerActor{}, nil
	default:
		return nil, errors.New("unknown actor type")
	}
}

func main() {
	grid.Logger = log.New(os.Stderr, "hellogrid", log.LstdFlags)

	address := flag.String("address", "", "bind address for gRPC")
	flag.Parse()

	etcd, err := etcdv3.New(etcdv3.Config{Endpoints: []string{"localhost:2379"}})
	successOrDie(err)

	client, err := grid.NewClient(etcd, grid.ClientCfg{Namespace: "hellogrid"})
	successOrDie(err)

	server, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: "hellogrid"}, HelloGrid{client})
	successOrDie(err)

	// Check for exit signal, ie: ctrl-c
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		fmt.Println("shutting down...")
		server.Stop()
		fmt.Println("shutdown complete")
	}()

	lis, err := net.Listen("tcp", *address)
	successOrDie(err)

	// The "leader" actor is special, it will automatically
	// get started for you when the Serve method is called.
	// The leader is always the entry-point. Even if you
	// start this app multiple times on different port
	// numbers there will only be one leader, it's a
	// singleton.
	err = server.Serve(lis)
	successOrDie(err)
}

func successOrDie(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
