package grid

import "context"

// Actor that does work.
type Actor interface {
	Act(c context.Context)
}
