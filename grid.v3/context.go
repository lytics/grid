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

// ContextForNonActor is useful for creating grid mailboxs in non actors.
// This use-case occurs when trying to establish bidirectional communication
// from a command-line-utility or from some existing service that does
// not use grid's actors.
//
// Example usage for a hypothetical cli tool that submits a task and gets
// the reply back to a mailbox it's listening to.
//
//     import (
//         ...
//
//         etcdv3 "github.com/coreos/etcd/clientv3"
//         "github.com/lytics/grid/grid.v3"
//     )
//
//     etcd, err := etcdv3.New(...)
//     ...
//
//     s, err := grid.NewServer(etcd, grid.ServerCfg{Namespace: "cli"}, nil)
//     ...
//
//     lis, err := net.Listener(...)
//     ...
//
//     go func() {
//         err := s.Serve(lis)
//         ...
//     }()
//
//     ctx := grid.ContextForNonActor(s)
//     go func() {
//         mailbox := grid.NewMailbox(ctx, "cli", 1)
//         defer mailbox.Close()
//         for {
//             select {
//             case <-ctx.Done():
//                 return
//             case m := <-mailbox.C:
//                 // Do something with message.
//             }
//         }
//     }()
//
//     c, err := grid.NewClient(etc, grid.ClientCfg{Namespace:"remote"})
//     ...
//
//     c.Request(timeout, "worker", &StartTask{})
//     ...
//
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
