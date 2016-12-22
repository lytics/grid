package grid

import (
	"context"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

// ContextActorID returns an ID that is a concatenation of the context
// namespace and the actor name associated with this context.
func ContextActorID(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return "", ErrInvalidContext
	}
	return cv.actorID, nil
}

// ContextActorName returns just the actor name, ie: no namespace, associated
// with this context.
func ContextActorName(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return "", ErrInvalidContext
	}
	return cv.actorName, nil
}

// ContextNamespace returns the namespace of the grid this actor
// is associated with.
func ContextNamespace(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return "", ErrInvalidContext
	}
	return cv.server.cfg.Namespace, nil
}

// ContextEtcd returns the etcd client for the grid this actor
// is associated with.
func ContextEtcd(c context.Context) (*etcdv3.Client, error) {
	v := c.Value(contextKey)
	if v == nil {
		return nil, ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return nil, ErrInvalidContext
	}
	return cv.server.etcd, nil
}

// ContextClient returns the grid client for the grid this actor
// is associated with.
func ContextClient(c context.Context) (*Client, error) {
	v := c.Value(contextKey)
	if v == nil {
		return nil, ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return nil, ErrInvalidContext
	}
	return cv.server.client, nil
}

// ContextForNonActor useful for creating grid mailboxs in non actors. This
// use case occurs when trying to establish bidirectional communication
// from a command-line-utility or from some existing service that does
// not use grid's actors interface. The server argument "s" will be
// listening on the gRPC port.
func ContextForNonActor(s *Server) context.Context {
	return context.WithValue(s.ctx, contextKey, &contextVal{
		server: s,
	})
}

// contextServer extracts the server from the context.
func contextServer(c context.Context) (*Server, error) {
	v := c.Value(contextKey)
	if v == nil {
		return nil, ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return nil, ErrInvalidContext
	}
	return cv.server, nil
}
