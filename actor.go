package grid

import "regexp"

type Actor interface {
	ID() string
	Act(g Grid, exit <-chan bool) bool
}

// ActorMaker makes actor instances using the provided definition. User
// code should implement this interface.
type ActorMaker interface {
	MakeActor(def *ActorDef) (Actor, error)
}

func NewActorDef(name string) *ActorDef {
	return &ActorDef{Name: name, Type: name, Settings: make(map[string]string), RawData: make(map[string][]byte)}
}

type ActorDef struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Settings map[string]string `json:"settings"`
	RawData  map[string][]byte `json:"rawdata"`
}

// ID returns the name of this actor definition.
func (a *ActorDef) ID() string {
	return a.Name
}

// DefineType defaults to actor name. Commonly used by the ActorMaker
// to switch on Type. This method is called if there are many actors
// with names like "consumer-0", "consumer-1", "consumer-2", etc.
// But all of them are really of "consumer" type.
func (a *ActorDef) DefineType(t string) *ActorDef {
	a.Type = t
	return a
}

// Define an arbitrary key and value setting. These are considered
// immutable once the actor has been started.
func (a *ActorDef) Define(k, v string) *ActorDef {
	a.Settings[k] = v
	return a
}

const validActorName = "^[a-zA-Z0-9-_]+$"

// IsValidActorName returns true if the give name matches the
// regular expression "^[a-zA-Z0-9-_]+$".
func IsValidActorName(name string) bool {
	if matched, err := regexp.MatchString(validActorName, name); err != nil {
		return false
	} else {
		return matched
	}
}
