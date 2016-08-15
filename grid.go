package grid

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/grid/balancer"
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
	dice         *rand.Rand
	name         string
	nodeid       string
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
	natsconnpool []*nats.EncodedConn
	maker        ActorMaker
}

func New(name, nodeid string, etcdservers []string, natsservers []string, m ActorMaker) Grid {
	return &grid{
		name:         name,
		nodeid:       nodeid,
		dice:         NewSeededRand(),
		etcdservers:  etcdservers,
		natsservers:  natsservers,
		mu:           new(sync.Mutex),
		stopped:      true,
		started:      false,
		exit:         make(chan bool),
		maker:        m,
		natsconnpool: make([]*nats.EncodedConn, 2*runtime.NumCPU()),
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
	return etcd.NewClient(g.etcdservers)
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

	if g.nodeid == "" {
		return nil, fmt.Errorf("nodeid cannot be empty")
	}

	// Define the metafora new task function and config.
	conf := m_etcd.NewConfig(g.nodeid, g.name, g.etcdservers)
	conf.NewTaskFunc = func(id, value string) metafora.Task {
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

		return newHandler(g.fork(), a)
	}

	// Create the metafora etcd coordinator.
	ec, err := m_etcd.NewEtcdCoordinator(conf)
	if err != nil {
		return nil, err
	}

	// Create the metafora consumer.
	b, err := balancer.New(g.name, g.nodeid, g.Etcd())
	if err != nil {
		return nil, err
	}
	c, err := metafora.NewConsumer(ec, handler(etcd.NewClient(g.etcdservers)), b)
	if err != nil {
		return nil, err
	}
	g.metaconsumer = c
	g.metaclient = m_etcd.NewClient(g.name, g.etcdservers)
	g.stopped = false
	g.started = true

	// Close the exit channel when metafora thinks
	// an exit is needed.
	go func() {
		defer close(g.exit)
		g.metaconsumer.Run()
	}()

	for i := 0; i < 2*runtime.NumCPU(); i++ {
		natsconn, err := g.newNatsConn()
		if err != nil {
			return nil, err
		}
		g.natsconnpool[i] = natsconn
		if i == 0 {
			g.natsconn = g.natsconnpool[0]
		}
	}

	return g.exit, nil
}

func (g *grid) fork() *grid {
	g.mu.Lock()
	defer g.mu.Unlock()

	return &grid{
		dice:         NewSeededRand(),
		name:         g.name,
		etcdservers:  g.etcdservers,
		natsservers:  g.natsservers,
		mu:           g.mu,
		started:      g.started,
		stopped:      g.stopped,
		exit:         g.exit,
		metaclient:   g.metaclient,
		metaconsumer: g.metaconsumer,
		natsconn:     g.natsconnpool[g.dice.Intn(2*runtime.NumCPU())],
		maker:        g.maker,
	}
}

func (g *grid) newNatsConn() (*nats.EncodedConn, error) {
	// Create a nats connection, un-encoded.
	natsop := nats.DefaultOptions
	natsop.Servers = g.natsservers
	natsnc, err := natsop.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %v, maybe use: %v", err, nats.DefaultURL)
	}
	// Create a nats connection, with encoding.
	natsconn, err := nats.NewEncodedConn(natsnc, nats.GOB_ENCODER)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoded connection: %v", err)
	}
	return natsconn, nil
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
