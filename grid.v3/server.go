package grid

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"log"
	"net"

	"time"

	"sync"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/registry"
	netcontext "golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	contextKey = "grid-context-key-xboKEsHA26"
)

type contextVal struct {
	server    *Server
	actorID   string
	actorName string
}

var (
	ErrInvalidContext    = errors.New("invalid context")
	ErrUnknownResponse   = errors.New("unknown response")
	ErrInvalidNamespace  = errors.New("invalid namespace")
	ErrAlreadyRegistered = errors.New("already registered")
)

var (
	Logger *log.Logger
)

// Server of a grid.
type Server struct {
	mu        sync.Mutex
	g         Grid
	etcd      *etcdv3.Client
	grpc      *grpc.Server
	registry  *registry.Registry
	namespace string
	mailboxes map[string]*Mailbox
	ctx       context.Context
	cancel    func()
}

// NewServer for the grid.
func NewServer(namespace string, etcd *etcdv3.Client, g Grid) (*Server, error) {
	if isNameValid(namespace) {
		return nil, ErrInvalidNamespace
	}
	if etcd == nil {
		return nil, ErrInvalidNamespace
	}
	return &Server{
		g:         g,
		etcd:      etcd,
		grpc:      grpc.NewServer(),
		namespace: namespace,
	}, nil
}

// Serve the grid on the listener.
func (s *Server) Serve(lis net.Listener) error {
	r, err := registry.New(s.etcd)
	if err != nil {
		return err
	}
	s.registry = r
	s.registry.Address = lis.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel

	mailbox, err := NewMailbox(nil, "", 10)
	if err != nil {
		return err
	}
	go s.runMailbox(mailbox)

	RegisterWireServer(s.grpc, s)
	return s.grpc.Serve(lis)
}

// Stop the server.
func (s *Server) Stop() {
	s.registry.Stop()
	s.grpc.Stop()
	s.cancel()
}

// Process a request and return a response. Implements the interface for
// gRPC definition of the wire service.
func (s *Server) Process(c netcontext.Context, req *Delivery) (*Delivery, error) {
	getMailbox := func() (*Mailbox, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		m, ok := s.mailboxes[req.Receiver]
		return m, ok
	}

	mailbox, ok := getMailbox()
	if !ok {
		return nil, ErrUnknownMailbox
	}

	// Write the bytes of the request into the byte
	// buffer for decoding.
	var buf bytes.Buffer
	n, err := buf.Write(req.Data)
	if err != nil {
		return nil, err
	}
	if n != len(req.Data) {
		return nil, io.ErrUnexpectedEOF
	}

	// Decode the request into an actual
	// type.
	env := &Envelope{}
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(env)
	if err != nil {
		return nil, err
	}
	env.context = context.WithValue(c, "", "")
	env.response = make(chan []byte)

	// Send the filled envelope to the actual
	// receiver. Also note that the receiver
	// can stop listenting when it wants, so
	// some defualt or timeout always needs
	// to exist here.
	select {
	case mailbox.c <- env:
	default:
		return nil, ErrReceiverBusy
	}

	// Wait for the receiver to send back a
	// reply, or the context to finish.
	select {
	case <-c.Done():
		return nil, ErrContextFinished
	case data := <-env.response:
		return &Delivery{
			Data: data,
		}, nil
	}
}

// runMailbox for this server.
func (s *Server) runMailbox(mailbox *Mailbox) {
	defer mailbox.Close()
	for {
		select {
		case <-s.ctx.Done():
			return
		case e := <-mailbox.C:
			switch msg := e.Msg.(type) {
			case *ActorDef:
				err := s.startActor(e.Context(), msg)
				if err != nil {
					e.Respond(&ResponseMsg{
						Succeeded: false,
						Error:     err.Error(),
					})
				} else {
					e.Respond(&ResponseMsg{
						Succeeded: true,
					})
				}
			}
		}
	}
}

// startActor in the current process. This method does not communicate with another
// system to choose where to run the actor. Calling this method will start the
// actor on the current host in the current process.
func (s *Server) startActor(c context.Context, def *ActorDef) error {
	def.namespace = s.namespace

	if err := ValidateActorDef(def); err != nil {
		return err
	}

	actor, err := s.g.MakeActor(def)
	if err != nil {
		return err
	}

	// Register the actor. This acts as a distributed mutex to
	// prevent an actor from starting twice on one system or
	// many systems.
	timeout, cancel := context.WithTimeout(c, 10*time.Second)
	err = s.registry.Register(timeout, def.regID())
	cancel()
	if err != nil {
		return err
	}

	// The actor's context contains its full id, it's name and the
	// full registration, which contains the actors namespace.
	actorCtx := context.WithValue(s.ctx, contextKey, &contextVal{
		server:    s,
		actorID:   def.ID(),
		actorName: def.Name,
	})

	// Start the actor, unregister the actor in case of failure
	// and capture panics that the actor raises.
	go func() {
		defer func() {
			timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			s.registry.Deregister(timeout, def.ID())
			cancel()
		}()
		defer func() {
			if err := recover(); err != nil {
				if Logger != nil {
					log.Printf("panic in actor: %v, recovered with: %v", def.ID(), err)
				}
			}
		}()
		actor.Act(actorCtx)
	}()

	return nil
}

// Mailbox for receiving messages.
type Mailbox struct {
	C       <-chan *Envelope
	c       chan *Envelope
	cleanup func() error
}

// Close the mailbox.
func (box *Mailbox) Close() error {
	return box.cleanup()
}

// NewMailbox for requests under the given receiver name.
func NewMailbox(c context.Context, name string, size int) (*Mailbox, error) {
	s, err := contextServer(c)
	if err != nil {
		return nil, err
	}

	_, ok := s.mailboxes[name]
	if ok {
		return nil, ErrAlreadyRegistered
	}

	timeout, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = s.registry.Register(timeout, name)
	cancel()
	if err != nil {
		return nil, err
	}

	cleanup := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Immediately delete the subscription so that no one
		// can send to it, at least from this host.
		delete(s.mailboxes, name)

		// Deregister the name.
		timeout, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := s.registry.Deregister(timeout, name)
		cancel()

		// Return any error from the deregister call.
		return err
	}
	boxC := make(chan *Envelope, size)
	box := &Mailbox{
		C:       boxC,
		c:       boxC,
		cleanup: cleanup,
	}
	s.mailboxes[name] = box
	return box, nil
}
