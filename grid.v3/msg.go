package grid

import (
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
)

var (
	ErrReceiverBusy          = errors.New("receiver busy")
	ErrUnknownMailbox        = errors.New("unknown mailbox")
	ErrContextFinished       = errors.New("context finished")
	ErrInvalidActorType      = errors.New("invalid actor type")
	ErrInvalidActorName      = errors.New("invalid actor name")
	ErrInvalidActorNamespace = errors.New("invalid actor namespace")
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
//
// The namespace does not need to be set. The grid server will
// overwrite the namespace to match its namespace.
//
func NewActorDef(name string, v ...interface{}) *ActorDef {
	fname := fmt.Sprintf(name, v...)
	return &ActorDef{
		Type: fname,
		Name: fname,
	}
}

// ActorDef defines the name and type of an actor.
type ActorDef struct {
	id        string
	Type      string
	Name      string
	Namespace string
}

// ID of the actor, includes both namespace and name.
func (a *ActorDef) ID() string {
	if a.id == "" {
		a.id = fmt.Sprintf("%v-%v", a.Namespace, a.Name)
	}
	return a.id
}

// regID (registration ID) of the actor, same as ID but with a hash suffix.
func (a *ActorDef) regID() string {
	h := fnv.New64()
	_, err := h.Write([]byte(a.ID()))
	if err != nil {
		panic("failed hashing actor id")
	}
	return fmt.Sprintf("%v-%v", a.ID(), h.Sum64())
}

// ValidateActorDef name and type.
func ValidateActorDef(def *ActorDef) error {
	if !isNameValid(def.Type) {
		return ErrInvalidActorType
	}
	if !isNameValid(def.Name) {
		return ErrInvalidActorName
	}
	if !isNameValid(def.Namespace) {
		return ErrInvalidActorNamespace
	}
	return nil
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
