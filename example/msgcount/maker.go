package main

import (
	"fmt"
	"strings"

	"github.com/lytics/grid"
)

type maker struct {
	conf Conf
}

func newActorMaker(conf Conf) (*maker, error) {
	if conf.NrReaders > 100 {
		return nil, fmt.Errorf("to many reader actors requested: %v", nreaders)
	}
	if conf.NrCounters > 100 {
		return nil, fmt.Errorf("to many counter actors requested: %v", ncounters)
	}
	return &maker{conf: conf}, nil
}

func (m *maker) MakeActor(id string) (grid.Actor, error) {
	switch {
	case strings.Contains(id, "reader"):
		return NewReaderActor(id, c.conf), nil
	case strings.Contains(id, "counter"):
		return NewCounterActor(id, m.conf), nil
	default:
		return nil, fmt.Errorf("name does not map to any type of actor: %v", id)
	}
}
