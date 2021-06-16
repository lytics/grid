package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lytics/grid/v3"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

const timeout = 2 * time.Second

// LeaderActor is the entry point of the application.
type LeaderActor struct {
	client *grid.Client
}

// Act checks for peers, ie: other processes running this code,
// in the same namespace and start the worker actor on one of them.
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
				if existing[peer.Name()] {
					continue
				}

				// Define a worker.
				existing[peer.Name()] = true
				start := grid.NewActorStart("worker-%d", len(existing))
				start.Type = "worker"

				// On new peers start the worker.
				_, err := a.client.Request(timeout, peer.Name(), start)
				successOrDie(err)
			}
		}
	}
}

// WorkerActor started by the leader.
type WorkerActor struct{}

// Act says hello and then waits for the exit signal.
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

func main() {
	logger := log.New(os.Stderr, "hellogrid: ", log.LstdFlags)

	address := flag.String("address", "", "bind address for gRPC")
	flag.Parse()

	// Connect to etcd.
	etcd, err := etcdv3.New(etcdv3.Config{Endpoints: []string{"localhost:2379"}})
	successOrDie(err)

	// Create a grid client.
	client, err := grid.NewClient(etcd, grid.ClientCfg{Namespace: "hellogrid", Logger: logger})
	successOrDie(err)

	// Create a grid server.
	server, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: "hellogrid", Logger: logger})
	successOrDie(err)

	// Define how actors are created.
	server.RegisterDef("leader", func(_ []byte) (grid.Actor, error) { return &LeaderActor{client: client}, nil })
	server.RegisterDef("worker", func(_ []byte) (grid.Actor, error) { return &WorkerActor{}, nil })

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
