package main

import (
	"fmt"
	"strings"

	"github.com/lytics/grid"
)

type maker struct {
	conf *Conf
}

func newActorMaker(conf *Conf) (*maker, error) {
	if conf.NrProducers > 100 {
		return nil, fmt.Errorf("to many producer actors requested: %v", conf.NrProducers)
	}
	if conf.NrConsumers > 100 {
		return nil, fmt.Errorf("to many consumer actors requested: %v", conf.NrConsumers)
	}
	return &maker{conf: conf}, nil
}

func (m *maker) MakeActor(id string) (grid.Actor, error) {
	switch {
	case strings.Contains(id, "leader"):
		return NewLeaderActor(id, m.conf), nil
	case strings.Contains(id, "producer"):
		return NewProducerActor(id, m.conf), nil
	case strings.Contains(id, "consumer"):
		return NewConsumerActor(id, m.conf), nil
	default:
		return nil, fmt.Errorf("name does not map to any type of actor: %v", id)
	}
}
