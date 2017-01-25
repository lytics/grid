package grid

// Grid of actors that can be created from their definitions.
type Grid interface {
	MakeActor(def *ActorDef) (Actor, error)
}

// MakerFunc that implements the Grid interface.
type MakerFunc func(def *ActorDef) (Actor, error)

// MakeActor makes an actor from its definition.
func (f MakerFunc) MakeActor(def *ActorDef) (Actor, error) {
	return f(def)
}