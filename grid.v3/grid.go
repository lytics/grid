package grid

// Grid of actors. MakeActor must always be able to
// create an actor with the following definition:
//
//    ActorDef {
//        Name: "leader",
//        Type: "leader",
//    }
//
// This leader actor is special and will automatically
// get started, as a singleton, when anyone calls the
// grid's Serve method.
type Grid interface {
	MakeActor(def *ActorDef) (Actor, error)
}
