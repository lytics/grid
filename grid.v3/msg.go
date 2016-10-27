package grid

import (
	"encoding/gob"
	"errors"
	"fmt"
)

var (
	ErrInvalidActorType      = errors.New("invalid actor type")
	ErrInvalidActorName      = errors.New("invalid actor name")
	ErrInvalidActorNamespace = errors.New("invalid actor namespace")
)

func NewActorDef(namespace, name string) *ActorDef {
	return &ActorDef{
		Type:      name,
		Name:      name,
		Namespace: namespace,
	}
}

type ActorDef struct {
	id        string
	Type      string
	Name      string
	Namespace string
}

// ID of the actor, in format of <namespace> . <name> all without whitespace.
func (a *ActorDef) ID() string {
	if a.id == "" {
		a.id = fmt.Sprintf("%v.%v", a.Namespace, a.Name)
	}
	return a.id
}

func ValidateActorDef(def *ActorDef) error {
	if !isNameValid(def.Name) {
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

type ResponseMsg struct {
	Succeeded bool
	Error     string
}

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
