package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lytics/grid"
	etcdv3 "github.com/coreos/etcd/clientv3"
)

var (
	grpcAddress string = "localhost:0"
	etcdServers string = "localhost:2379"
	httpAddress string = "localhost:8080"
)

const timeout = 2 * time.Second

// LeaderActor is the scheduler to create and watch
// the workers but the work comes from http requests
type LeaderActor struct {
	client *grid.Client
}

// Act checks for peers, ie: other processes running this code,
// in the same namespace and start the worker actor on one of them.
func (a *LeaderActor) Act(c context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	fmt.Println("Starting Leader Actor")

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
type WorkerActor struct {
	server *grid.Server
}

// Act listens for messages from an http agent, and
// does its work and responds.
func (a *WorkerActor) Act(ctx context.Context) {

	name, _ := grid.ContextActorName(ctx)

	fmt.Printf("starting %q\n", name)

	// Listen to a mailbox with the same
	// name as the actor.
	mailbox, err := grid.NewMailbox(a.server, name, 10)
	successOrDie(err)
	defer mailbox.Close()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("goodbye...")
			return
		case req, ok := <-mailbox.C:
			if !ok {
				return
			}
			switch req.Msg().(type) {
			case *Event:
				fmt.Printf("msg %+v\n", req.Msg())
				err := req.Respond(&EventResponse{Id: name})
				if err != nil {
					fmt.Printf("error on message response %v\n", err)
				}
			default:
				fmt.Printf("ERROR:  wrong type %#v", req.Msg())
			}
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	logger := log.New(os.Stderr, "requestreply: ", log.LstdFlags)

	flag.StringVar(&grpcAddress, "grpcaddress", grpcAddress, "bind address for gRPC")
	flag.StringVar(&etcdServers, "etcd", etcdServers, "etcd servers, comma delimited")
	flag.StringVar(&httpAddress, "httpaddress", httpAddress, "http listener address")
	flag.Parse()

	// Register our Message Types.
	grid.Register(Event{})
	grid.Register(EventResponse{})

	// Connect to etcd.
	etcd, err := etcdv3.New(etcdv3.Config{Endpoints: strings.Split(etcdServers, ",")})
	successOrDie(err)

	// Create a grid client.
	client, err := grid.NewClient(etcd, grid.ClientCfg{Namespace: "reqrep", Logger: logger})
	successOrDie(err)

	// Create a grid server.
	server, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: "reqrep", Logger: logger})
	successOrDie(err)

	// Define how actors are created.
	server.RegisterDef("leader", func(_ []byte) (grid.Actor, error) { return &LeaderActor{client: client}, nil })
	server.RegisterDef("worker", func(_ []byte) (grid.Actor, error) { return &WorkerActor{server: server}, nil })

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		fmt.Println("shutting down...")
		server.Stop()
		fmt.Println("shutdown complete")
	}()

	// Only one http listener per Node, as it binds to port
	// 2nd will die quietly, which is fine
	api := NewApi(client)
	go api.Run()

	lis, err := net.Listen("tcp", grpcAddress)
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

type apiServer struct {
	c                *grid.Client
	ctx              context.Context
	peers            map[string]bool
	workerCt         int
	mu               sync.Mutex
	workerLoadTicker *time.Ticker
}

func NewApi(c *grid.Client) *apiServer {
	a := &apiServer{c: c}
	a.ctx = context.Background()
	a.workerLoadTicker = time.NewTicker(2 * time.Second)
	return a
}

// Keep watching for changes to workers
func (m *apiServer) loadWorkers() {

	defer m.workerLoadTicker.Stop()

	// TODO:  use querywatch instead
	// m.c.QueryWatch(ctx, filter)

	for {
		select {
		case <-m.ctx.Done():
			return
		case _, ok := <-m.workerLoadTicker.C:
			if !ok {
				fmt.Println("Got exit in ticket")
				return
			}
			// Ask for current peers.
			peers, err := m.c.Query(timeout, grid.Peers)
			successOrDie(err)
			existing := make(map[string]bool)
			m.mu.Lock()
			for _, peer := range peers {
				existing[peer.Name()] = true
			}
			m.peers = existing
			m.workerCt = len(existing)
			fmt.Println("found peers ", m.peers)
			m.mu.Unlock()
		}
	}
}

func (m *apiServer) ConsistentWorker(user string) string {
	h := fnv.New32a()
	h.Write([]byte(user))
	val := h.Sum32()
	fmt.Printf("val: %d   modulo:%d \n\n", val, val%uint32(m.workerCt))
	return fmt.Sprintf("worker-%d", (val%uint32(m.workerCt))+1)
}

func (m *apiServer) RandomWorker() string {
	val := rand.Int31n(int32(m.workerCt))
	return fmt.Sprintf("worker-%d", val+1)
}

func (m *apiServer) Run() {

	// Ensure we have a current list of workers.
	go m.loadWorkers()

	http.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("in /work handler")

		// Use data in request to use consistent hashing
		// to ensure requests get routed
		// to same server for same xxxx in request

		user := r.URL.Query().Get("user")
		var worker string
		if user == "" {
			user = "World"
			worker = m.RandomWorker()
		} else {
			worker = m.ConsistentWorker(user)
		}

		res, err := m.c.Request(timeout, worker, &Event{User: user})
		fmt.Printf("request user: %q   response: %#v  err=%v\n", user, res, err)
		if er, ok := res.(*EventResponse); ok {
			fmt.Fprintf(w, "Response %s\n\n", er.Id)
		} else {
			fmt.Fprintf(w, "wrong response type")
		}
	})

	err := http.ListenAndServe(httpAddress, nil)
	if err != nil {
		// In event this server/node already has other
		// workers, we won't run api on this one as we
		// only need one per server.
		m.workerLoadTicker.Stop()
	}
}
