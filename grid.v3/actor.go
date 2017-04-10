package grid

import "context"

// MakeActor using the given data to parameterize
// the making of the actor; the data is optional.
type MakeActor func(data []byte) (Actor, error)

// Actor that does work.
type Actor interface {
	Act(c context.Context)
}
