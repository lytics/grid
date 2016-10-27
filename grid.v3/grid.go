package grid

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/lytics/grid/grid.v3/discovery"
	"github.com/lytics/grid/grid.v3/message"
)

const (
	contextKey = "grid-context-key-xboKEsHA26"
)

type contextVal struct {
	r       *registration
	actorID string
}

var (
	ErrInvalidContext    = errors.New("invalid context")
	ErrInvalidNamespace  = errors.New("invalid namespace")
	ErrAlreadyRegistered = errors.New("already registered")
)

var (
	Logger *log.Logger
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
	mm *message.Messenger
	co *discovery.Coordinator
}

func (r *registration) ID() string {
	return r.id
}

type Grid interface {
	Namespace() string
	MakeActor(def *ActorDefMsg) (Actor, error)
}

func RegisterGrid(co *discovery.Coordinator, mm *message.Messenger, g Grid) error {
	mu.Lock()
	defer mu.Unlock()

	r, ok := registry[g.Namespace()]
	if ok {
		return ErrAlreadyRegistered
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	r = &registration{
		id: fmt.Sprintf("/%v/registration/%v", g.Namespace(), hostname),
		g:  g,
		co: co,
		mm: mm,
	}

	timeout, cancel := context.WithTimeout(co.Context(), 10*time.Second)
	err = co.Register(timeout, r.ID())
	cancel()
	if err != nil {
		return err
	}

	timeout, cancel = context.WithTimeout(co.Context(), 10*time.Second)
	sub, err := mm.Subscribe(co.Context(), g.Namespace(), hostname, 10)
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
				case ActorDefMsg:
					err := StartActor(&msg)
					if err != nil {
						e.Respond(&ResponseMsg{
							Succeeded: false,
							Error:     err.Error(),
						})
					} else {
						e.Respond(&ResponseMsg{
							Succeeded: true,
						})
					}
				}
			}
		}
	}()

	return nil
}

// StartActor in the current process. This method does not
// communicate or RPC with another system to choose where
// to run the actor. Calling this method will start the
// actor in the current process.
func StartActor(def *ActorDefMsg) error {
	mu.Lock()
	defer mu.Unlock()

	r, ok := registry[def.Namespace]
	if !ok {
		return ErrInvalidNamespace
	}

	c := context.WithValue(r.co.Context(), contextKey, &contextVal{r: r, actorID: def.ID()})
	actor, err := r.g.MakeActor(def)
	if err != nil {
		return err
	}

	timeout, cancel := context.WithTimeout(r.co.Context(), 10*time.Second)
	err = r.co.Register(timeout, def.ID())
	cancel()
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = r.co.Deregister(timeout, def.ID())
			cancel()
		}()
		defer func() {
			if r := recover(); r != nil {
				if Logger != nil {
					log.Printf("panic in actor: %v, recovered with: %v", def.ID(), r)
				}
			}
		}()
		actor.Act(c)
	}()

	return nil
}
