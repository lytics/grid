package grid

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/m_etcd"
	"github.com/nats-io/nats"
)

const (
	BuffSize = 8000
)

type Grid interface {
	Start() (<-chan bool, error)
	Stop()
	NewFlow(name string, roles ...string) (Flow, error)
	RegisterActor(role string, n int, f NewActor)
}

type grid struct {
	name        string
	etcdconnect []string
	natsconnect []string
	mu          *sync.Mutex
	started     bool
	stopped     bool
	exit        chan bool
	actorcnt    map[string]int
	actornew    map[string]NewActor
	client      metafora.Client
	consumer    *metafora.Consumer
	natsec      *nats.EncodedConn
}

func New(name string, etcdconnect []string, natsconnect []string) Grid {
	return &grid{
		name:        name,
		etcdconnect: etcdconnect,
		natsconnect: natsconnect,
		mu:          new(sync.Mutex),
		started:     false,
		stopped:     true,
		exit:        make(chan bool),
		actorcnt:    make(map[string]int),
		actornew:    make(map[string]NewActor),
	}
}

func (g *grid) Start() (<-chan bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.started {
		return g.exit, nil
	}

	ec, err := m_etcd.NewEtcdCoordinator("coordinator", g.name, g.etcdconnect)
	if err != nil {
		return nil, err
	}

	ec.NewTask = func(id, value string) metafora.Task {
		receiver, err := newActorTaskFromString(id)
		if err != nil {
			log.Printf("error: %v", err)
			return nil
		}
		if f, ok := g.actornew[receiver.Role]; ok {
			return newConn(g, receiver, f(id, value))
		} else {
			log.Printf("error: failed to schedule actor: %v: nothing registered for role: %v", id, receiver.Role)
			return nil
		}
	}

	c, err := metafora.NewConsumer(ec, handler(etcd.NewClient(g.etcdconnect)), m_etcd.NewFairBalancer("balancer", g.name, g.etcdconnect))
	if err != nil {
		return nil, err
	}
	g.consumer = c
	g.client = m_etcd.NewClient(g.name, g.etcdconnect)

	go func() {
		defer close(g.exit)
		g.consumer.Run()
	}()

	natsnc, err := nats.Connect(strings.Join(g.natsconnect, ","))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %v, maybe user: %v", err, nats.DefaultURL)
	}
	natsec, err := nats.NewEncodedConn(natsnc, nats.GOB_ENCODER)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoded connection: %v", err)
	}
	g.natsec = natsec

	return g.exit, nil
}

func (g *grid) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.stopped {
		g.consumer.Shutdown()
		g.stopped = true
	}
}

func (g *grid) NewFlow(name string, roles ...string) (Flow, error) {
	f := newFlow(name, g)
	for _, role := range roles {
		n, ok := g.actorcnt[role]
		if !ok {
			return nil, fmt.Errorf("role not defined: %v", role)
		}
		f.conf[role] = n
	}
	return f, nil
}

func (g *grid) RegisterActor(role string, n int, f NewActor) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.actorcnt[role] = n
	g.actornew[role] = f
}

func handler(c *etcd.Client) metafora.HandlerFunc {
	return func(t metafora.Task) metafora.Handler {
		return t.(*conn)
	}
}
