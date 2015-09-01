package grid

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd"
	"github.com/nats-io/nats"
)

type Grid interface {
	Start() (<-chan bool, error)
	Stop()
	Name() string
	StartActor(def *ActorDef) error
	Nats() *nats.EncodedConn
	Etcd() *etcd.Client
}

type grid struct {
	name         string
	etcdservers  []string
	natsservers  []string
	mu           *sync.Mutex
	started      bool
	stopped      bool
	exit         chan bool
	etcdclient   *etcd.Client
	metaclient   metafora.Client
	metaconsumer *metafora.Consumer
	natsconn     *nats.EncodedConn
	maker        ActorMaker
}

func New(name string, etcdservers []string, natsservers []string, m ActorMaker) Grid {
	return &grid{
		name:        name,
		etcdservers: etcdservers,
		natsservers: natsservers,
		mu:          new(sync.Mutex),
		stopped:     true,
		exit:        make(chan bool),
		maker:       m,
	}
}

// Nats connection usable by any actor running
// in the grid.
func (g *grid) Nats() *nats.EncodedConn {
	return g.natsconn
}

// Etcd connection usable by any actor running
// in the grid.
func (g *grid) Etcd() *etcd.Client {
	return g.etcdclient
}

// Name of the grid.
func (g *grid) Name() string {
	return g.name
}

// Start the grid. Actors that were stopped from a previous exit
// of the grid but returned a "done" status of false will start
// to be scheduled. New actors can be scheduled with StartActor.
func (g *grid) Start() (<-chan bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Start only once.
	if g.started {
		return g.exit, nil
	}

	// Use the hostname as the node identifier.
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	// Create the metafora etcd coordinator.
	ec, err := m_etcd.NewEtcdCoordinator(hostname, g.name, g.etcdservers)
	if err != nil {
		return nil, err
	}

	// Define the metafora new task function.
	ec.NewTask = func(id, value string) metafora.Task {
		def := NewActorDef(id)
		err := json.Unmarshal([]byte(value), def)
		if err != nil {
			log.Printf("error: failed to schedule actor: %v, error: %v", id, err)
			return nil
		}
		a, err := g.maker.MakeActor(def)
		if err != nil {
			log.Printf("error: failed to schedule actor: %v, error: %v", id, err)
			return nil
		}
		return newHandler(g, a)
	}

	// Create the metafora consumer.
	c, err := metafora.NewConsumer(ec, handler(etcd.NewClient(g.etcdservers)), m_etcd.NewFairBalancer(hostname, g.name, g.etcdservers))
	if err != nil {
		return nil, err
	}
	g.metaconsumer = c
	g.metaclient = m_etcd.NewClient(g.name, g.etcdservers)

	// Close the exit channel when metafora thinks
	// an exit is needed.
	go func() {
		defer close(g.exit)
		g.metaconsumer.Run()
	}()

	// Create a nats connection, un-encoded.
	natsop := nats.DefaultOptions
	natsop.Servers = g.natsservers
	natsnc, err := natsop.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %v, maybe user: %v", err, nats.DefaultURL)
	}
	// Create a nats connection, with encoding.
	natsconn, err := nats.NewEncodedConn(natsnc, nats.GOB_ENCODER)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoded connection: %v", err)
	}
	g.natsconn = natsconn

	// Create an etcd client, this is the "raw" etcd
	// client.
	g.etcdclient = etcd.NewClient(g.etcdservers)

	return g.exit, nil
}

// Stop the grid. Asks all actors to exit. Actors that return
// a "done" status of false will remain scheduled, and will
// start once the grid is started again without a need to
// call StartActor.
func (g *grid) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.stopped {
		g.metaconsumer.Shutdown()
		g.stopped = true
	}
}

// StartActor starts one actor of the given name, if the actor is already
// running no error is returned.
func (g *grid) StartActor(def *ActorDef) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	err := g.metaclient.SubmitTask(def)
	if err != nil {
		switch err := err.(type) {
		case *etcd.EtcdError:
			// If the error code is 105, this means the task is
			// already in etcd, which could be possible after
			// a crash or after the actor exits but returns
			// true to be kept as an entry for metafora to
			// schedule again.
			if err.ErrorCode != 105 {
				return err
			}
		default:
			return err
		}
	}
	return nil
}

// The handler implements both metafora.Task, and metafora.Handler.
func handler(c *etcd.Client) metafora.HandlerFunc {
	return func(t metafora.Task) metafora.Handler {
		return t.(*actorhandler)
	}
}
