package grid

import (
	"errors"
	"fmt"
	"regexp"

	"golang.org/x/net/context"
)

var (
	ErrInvalidActorName      = errors.New("invalid actor name")
	ErrInvalidActorNamespace = errors.New("invalid actor namespace")
)

type Actor interface {
	Act(c context.Context)
}

func NewActorDef(namespace, name string) (*ActorDef, error) {
	if !isNameValid(name) {
		return nil, ErrInvalidActorName
	}
	if !isNameValid(namespace) {
		return nil, ErrInvalidActorNamespace
	}
	return &ActorDef{
		Type:      name,
		Name:      name,
		Namespace: namespace,
	}, nil
}

type ActorDef struct {
	id        string
	Type      string `json:"type"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// ID of the actor, in format of /<namespace>/<name>
func (a *ActorDef) ID() string {
	if a.id == "" {
		a.id = fmt.Sprintf("/%v/%v", a.Namespace, a.Name)
	}
	return a.id
}

// DefineType defaults to actor name. Commonly used by the ActorMaker
// to switch on Type. This method is called if there are many actors
// with names like "consumer-0", "consumer-1", "consumer-2", etc.
// But all of them are really of "consumer" type.
func (a *ActorDef) DefineType(t string) *ActorDef {
	a.Type = t
	return a
}

const validActorName = "^[a-zA-Z0-9-_]+$"

// IsNameValid returns true if the give name matches the
// regular expression "^[a-zA-Z0-9-_]+$".
func isNameValid(name string) bool {
	if matched, err := regexp.MatchString(validActorName, name); err != nil {
		return false
	} else {
		return matched
	}
}
