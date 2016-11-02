package grid

// Grid of actors.
type Grid interface {
	MakeActor(def *ActorDef) (Actor, error)
}
