package grid

import (
	"github.com/lytics/grid/grid.v3/discovery"
	"github.com/lytics/grid/grid.v3/messenger"
	"golang.org/x/net/context"
)

func ContextNamespace(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	r, ok := c.(*registration)
	if !ok {
		return "", ErrInvalidContext
	}
	return r.g.Namespace
}

func ContextMessenger(c context.Context) (*messenger.Nexus, error) {
	v := c.Value(contextKey)
	if v == nil {
		return nil, ErrInvalidContext
	}
	r, ok := c.(*registration)
	if !ok {
		return nil, ErrInvalidContext
	}
	return r.nx
}

func ContextCoordinator(c context.Context) (*discovery.Coordinator, error) {
	v := c.Value(contextKey)
	if v == nil {
		return nil, ErrInvalidContext
	}
	r, ok := c.(*registration)
	if !ok {
		return nil, ErrInvalidContext
	}
	return r.co
}
