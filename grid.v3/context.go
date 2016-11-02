package grid

import (
	"context"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

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

func ContextNamespace(c context.Context) (string, error) {
	v := c.Value(contextKey)
	if v == nil {
		return "", ErrInvalidContext
	}
	cv, ok := v.(*contextVal)
	if !ok {
		return "", ErrInvalidContext
	}
	return cv.server.namespace, nil
}

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
