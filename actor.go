package grid

import (
	"context"
	"fmt"
	"sync"
)

func init() {
	Register(Ack{})
	Register(ActorStart{})
}

// MakeActor using the given data to parameterize
// the making of the actor; the data is optional.
type MakeActor func(data []byte) (Actor, error)

// Actor that does work.
type Actor interface {
	Act(c context.Context)
}

// NewActorStart message with the name of the actor
// to start, its type will be equal to its name
// unless its changed:
//
//     start := NewActorStart("worker")
//
// Format names can also be used for more complicated
// names, just remember to override the type:
//
//     start := NewActorStart("worker-%d-group-%d", i, j)
//     start.Type = "worker"
//
func NewActorStart(name string, v ...interface{}) *ActorStart {
	fullName := name
	if len(v) > 0 {
		fullName = fmt.Sprintf(name, v...)
	}
	return &ActorStart{
		Type: fullName,
		Name: fullName,
	}
}

type makeActorRegistry struct {
	mu sync.RWMutex
	r  map[string]MakeActor
}

func newMakeActorRegistry() *makeActorRegistry {
	return &makeActorRegistry{
		r: make(map[string]MakeActor),
	}
}

func (r *makeActorRegistry) Get(name string) (m MakeActor, found bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, found = r.r[name]
	return
}

func (r *makeActorRegistry) Set(name string, m MakeActor) (update bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, update = r.r[name]
	r.r[name] = m
	return
}
