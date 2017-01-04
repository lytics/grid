package grid

import (
	"encoding/gob"
	"errors"
	"fmt"
)

// NewActorDef with name. The name can be a format string and its
// arguments. For example:
//
//     def := NewActorDef("leader")
//
// or
//
//     def := NewActorDef("worker-%d", i)
//     def.Type = "worker"
//
// Remeber that you will likely need to set the "Type" of actor
// when using a format strings with arguments, like in the
// example above.
//
// The name must contain only characters in the set: [a-zA-Z0-9-_]
func NewActorDef(name string, v ...interface{}) *ActorDef {
	fname := fmt.Sprintf(name, v...)
	return &ActorDef{
		Type: fname,
		Name: fname,
	}
}

// ActorDef defines the name and type of an actor.
type ActorDef struct {
	Type string
	Name string
}

// ResponseMsg for generic responses that need only a
// success flag or error.
type ResponseMsg struct {
	Succeeded bool
	Error     string
}

// Err retured in response if any, nil otherwise.
func (m *ResponseMsg) Err() error {
	if !m.Succeeded && m.Error != "" {
		return errors.New(m.Error)
	}
	return nil
}

func init() {
	gob.Register(&ResponseMsg{})
	gob.Register(&ActorDef{})
}
