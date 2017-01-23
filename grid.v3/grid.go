package grid

// Grid of actors that can be created from their definitions.
type Grid interface {
	MakeActor(def *ActorDef) (Actor, error)
}

// Func that implements the Grid interface.
type Func func(def *ActorDef) (Actor, error)

// MakeActor calls the grid func to create an actor.
func (f Func) MakeActor(def *ActorDef) (Actor, error) {
	return f(def)
}