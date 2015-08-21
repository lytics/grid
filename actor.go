package grid

import "fmt"

type Actor interface {
	ID() string
	Act(g Grid, exit <-chan bool) bool
}

type ActorMaker interface {
	MakeActor(name string) (Actor, error)
}

type ActorName struct {
	Name string
}

func NewActorName(grid, actor string) *ActorName {
	return &ActorName{Name: fmt.Sprintf("%s.%s", grid, actor)}
}

func newActorNameFromString(name string) *ActorName {
	return &ActorName{Name: name}
}

func (a *ActorName) ID() string {
	return a.Name
}
