package grid

import (
	"fmt"
	"strconv"
	"strings"
)

type Actor interface {
	ID() string
	Act(c Conn) bool
}

type NewActor func(id, state string) Actor

type actorTask struct {
	Role string
	Flow string
	Part int
}

func newActorTask(flow, role string, part int) (*actorTask, error) {
	if strings.Contains(flow, ".") {
		return nil, fmt.Errorf("flow name may not contain dot: %v", flow)
	}
	if strings.Contains(role, ".") {
		return nil, fmt.Errorf("role name may not contain dot: %v", role)
	}
	if part < 0 {
		return nil, fmt.Errorf("part must be positive: %v", part)
	}
	return &actorTask{Flow: flow, Role: role, Part: part}, nil
}

func newActorTaskFromString(id string) (*actorTask, error) {
	ps := strings.Split(id, ".")
	if len(ps) != 3 {
		return nil, fmt.Errorf("failed to split id into exactly three units, flow, group, part: %v", id)
	}
	flow := ps[0]
	role := ps[1]
	partstr := ps[2]

	part, err := strconv.Atoi(partstr)
	if err != nil {
		return nil, err
	}

	return newActorTask(flow, role, part)
}

func (a *actorTask) ID() string {
	return fmt.Sprintf("%v.%v.%d", a.Flow, a.Role, a.Part)
}
