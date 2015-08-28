package grid

type Actor interface {
	ID() string
	Act(g Grid, exit <-chan bool) bool
}

type ActorMaker interface {
	MakeActor(name string) (Actor, error)
}

type ActorName string

func (a ActorName) ID() string {
	return string(a)
}
