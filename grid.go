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
	StartActor(name string) error
	Nats() *nats.EncodedConn
	Etcd() *etcd.Client
}

type grid struct {
	name         string
	etcdconnect  []string
	natsconnect  []string
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

func New(name string, etcdconnect []string, natsconnect []string, m ActorMaker) Grid {
	return &grid{
		name:        name,
		etcdconnect: etcdconnect,
		natsconnect: natsconnect,
		mu:          new(sync.Mutex),
		stopped:     true,
		exit:        make(chan bool),
		maker:       m,
	}
}

func (g *grid) Nats() *nats.EncodedConn {
	return g.natsconn
}

func (g *grid) Etcd() *etcd.Client {
	return g.etcdclient
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
		receiver := newActorNameFromString(id)
		a, err := g.maker.MakeActor(receiver.ID())
		if err != nil {
			log.Printf("error: failed to schedule actor: %v, error: %v", id, err)
			return nil
		}
		return newHandler(g, a)
	}

	c, err := metafora.NewConsumer(ec, handler(etcd.NewClient(g.etcdconnect)), m_etcd.NewFairBalancer("balancer", g.name, g.etcdconnect))
	if err != nil {
		return nil, err
	}
	g.metaconsumer = c
	g.metaclient = m_etcd.NewClient(g.name, g.etcdconnect)

	go func() {
		defer close(g.exit)
		g.metaconsumer.Run()
	}()

	natsnc, err := nats.Connect(strings.Join(g.natsconnect, ","))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %v, maybe user: %v", err, nats.DefaultURL)
	}
	natsconn, err := nats.NewEncodedConn(natsnc, nats.GOB_ENCODER)
	if err != nil {
		return nil, fmt.Errorf("failed to create encoded connection: %v", err)
	}
	g.natsconn = natsconn

	g.etcdclient = etcd.NewClient(g.etcdconnect)

	return g.exit, nil
}

func (g *grid) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.stopped {
		g.metaconsumer.Shutdown()
		g.stopped = true
	}
}

func handler(c *etcd.Client) metafora.HandlerFunc {
	return func(t metafora.Task) metafora.Handler {
		return t.(*actorhandler)
	}
}

func (g *grid) StartActor(name string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	err := g.metaclient.SubmitTask(NewActorName(g.name, name))
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
