package grid

// Grid of actors. MakeActor must always be able to
// create an actor with the following definition:
//
//    ActorDef {
//        Name: "leader",
//        Type: "leader",
//    }
//
type Grid interface {
	MakeActor(def *ActorDef) (Actor, error)
}
