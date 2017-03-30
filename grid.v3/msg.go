package grid

import (
	"encoding/gob"
	"fmt"

	"github.com/lytics/grid/grid.v3/codec"
)

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

// ActorStart can be sent via the grid client to
// a peer to start that actor on that peer.
//
// Both fields Type and Name must contain only
// characters in the set: [a-zA-Z0-9-_]
//
// The data in the field Data is passed to the
// function passed to the server's RegisterDef
// method.
type ActorStart struct {
	Type string
	Name string
	Data []byte
}

func init() {
	gob.Register(&ActorStart{})
	codec.Registry().Register(&ActorStart{}, &codec.GobCodec{})
}
