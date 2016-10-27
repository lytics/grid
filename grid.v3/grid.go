package grid

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/lytics/grid/grid.v3/discovery"
	"github.com/lytics/grid/grid.v3/messenger"
	"golang.org/x/net/context"
)

const (
	contextKey = "grid-context-data"
)

var (
	ErrInvalidContext        = errors.New("invalid context")
	ErrInvalidNamespace      = errors.New("invalid namespace")
	ErrGridAlreadyRegistered = errors.New("grid already registered")
)

var (
	mu       *sync.Mutex
	dice     *rand.Rand
	registry map[string]*registration
)

func init() {
	mu = &sync.Mutex{}
	registry = make(map[string]*registration)

	r, err := NewSeededRand()
	if err != nil {
		panic(err.Error())
	}
	dice = r
}

type registration struct {
	id string
	g  Grid
	nx *messenger.Nexus
	co *discovery.Coordinator
}

func (r *registration) ID() string {
	return r.id
}

type Grid interface {
	Namespace() string
	MakeActor(def *ActorDef) (Actor, error)
}

func RegisterGrid(co *discovery.Coordinator, nx *messenger.Nexus, g Grid) error {
	mu.Lock()
	defer mu.Unlock()

	r, ok := registry[g.Namespace]
	if ok {
		return ErrGridAlreadyRegistered
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	r = &registration{
		id: fmt.Sprintf("/%v/registration/%v", g.Namespace(), hostname),
		g:  g,
		co: co,
	}

	timeout, cancel := context.WithTimeout(co.Context(), 10*time.Second)
	err = co.Register(timeout, r.ID())
	cancel()
	if err != nil {
		return err
	}

	timeout, cancel := context.WithTimeout(co.Context(), 10*time.Second)
	sub, err := nx.Subscribe(co, g.Namespace(), hostname, 10)
	cancel()
	if err != nil {
		return err
	}

	registry[g.Namespace()] = r

	go func() {
		for {
			select {
			case <-co.Context().Done():
				timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				sub.Unsubscribe(timeout)
				cancel()
			case e := <-sub.Mailbox():
				switch msg := e.Msg.(type) {
				case ActorDef:
					err := StartActor(&msg)
					if err != nil {
						e.Respond()
					}
				}
			}
		}
	}()

	return nil
}

func StartActor(def *ActorDef) error {
	mu.Lock()
	defer mu.Unlock()

	r, ok := registry[def.Namespace]
	if !ok {
		return ErrInvalidNamespace
	}

	c := context.WithValue(r.co.Context(), contextKey, r)
	actor, err := r.g.MakeActor(def)
	if err != nil {
		return err
	}

	timeout, cancel := context.WithTimeout(r.co.Context(), 10*time.Second)
	err = r.co.Register(timeout, actor.ID())
	cancel()
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = r.co.Deregister(timeout, actor.ID())
			cancel()
		}()
		actor.Act(c)
	}()

	return nil
}
