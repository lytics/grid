package grid

// Grid of actors that can be created from their definitions.
type Grid interface {
	MakeActor(def *ActorDef) (Actor, error)
}
