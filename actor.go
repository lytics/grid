package grid

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
	return &ActorDef{Name: name, Type: name, Settings: make(map[string]string)}
}

type ActorDef struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Settings map[string]string `json:"settings"`
}

// ID returns the name of this actor definition.
func (a *ActorDef) ID() string {
	return a.Name
}

// DefineType defaults to actor name. Commonly used by the ActorMaker
// to switch on Type. Can we changed by user code if needed.
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
