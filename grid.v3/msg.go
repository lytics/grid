package grid

import (
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

type ackmsg struct {
	ok string
}

func (a *ackmsg) Marshal(v interface{}) ([]byte, error) {
	return []byte("__ACK__"), nil //This will be the wire message for debugging but it'll just be thrown away in Unmarshal.
}

func (a *ackmsg) Unmarshal(data []byte, v interface{}) error {
	if _, ok := v.(*ackmsg); !ok {
		return fmt.Errorf("incorrect codec, not of type ackmsg")
	}
	v = a
	return nil
}

func (a *ackmsg) BlankSlate() interface{} {
	return a
}

func (a *ackmsg) String() string {
	return "ackmsg"
}

// Ack is the message sent back when the Ack() method of a
var Ack = &ackmsg{"__ACK__"}

func init() {
	//ActorStart messages aren't expected in replies but this interface still requies an instance constructor to register the type.
	codec.GobCodecRegister(codec.Registry(), func() interface{} { return &ActorStart{} })
	codec.Registry().Register(Ack, Ack) // Ack is the message and the Codec for itself.
}
