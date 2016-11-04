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
//
// Inside the MakeActor method a switch statement around
// the def.Type can be used to decide which type of actor
// to create.
//
//     MakeActor(def *ActorDer) (Actor, error) {
//         switch def.Type {
//         case "leader":
//             ...
//         case "worker":
//             ...
//         default:
//             return nil, errors.New("unknown actor type")
//     }
//
type Grid interface {
	MakeActor(def *ActorDef) (Actor, error)
}
