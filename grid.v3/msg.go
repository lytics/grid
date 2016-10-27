package grid

import (
	"encoding/gob"
	"errors"
	"fmt"
)

func NewActorDefMsg(namespace, name string) (*ActorDefMsg, error) {
	if !isNameValid(name) {
		return nil, ErrInvalidActorName
	}
	if !isNameValid(namespace) {
		return nil, ErrInvalidActorNamespace
	}
	return &ActorDefMsg{
		Type:      name,
		Name:      name,
		Namespace: namespace,
	}, nil
}

type ActorDefMsg struct {
	id        string
	Type      string `json:"type"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// ID of the actor, in format of /<namespace>/<name>
func (a *ActorDefMsg) ID() string {
	if a.id == "" {
		a.id = fmt.Sprintf("/%v/%v", a.Namespace, a.Name)
	}
	return a.id
}

// DefineType defaults to actor name. Commonly used by the ActorMaker
// to switch on Type. This method is called if there are many actors
// with names like "consumer-0", "consumer-1", "consumer-2", etc.
// But all of them are really of "consumer" type.
func (a *ActorDefMsg) DefineType(t string) *ActorDefMsg {
	a.Type = t
	return a
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
	gob.Register(&ActorDefMsg{})
}
