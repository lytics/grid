package grid

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

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

// Logger used for logging when non-nil, default is nil.
var Logger *log.Logger

// Server of a grid.
type Server struct {
	mu        sync.Mutex
	g         Grid
	cfg       ServerCfg
	etcd      *etcdv3.Client
	grpc      *grpc.Server
	registry  *registry.Registry
	mailboxes map[string]*Mailbox
	client    *Client
	ctx       context.Context
	cancel    func()
}

// NewServer for the grid. The namespace must contain only characters
// in the set: [a-zA-Z0-9-_] and no other.
//
// If argument g is nil, then this server will not create actors, will
// not start a leader, and can be used only for serving mailboxes.
func NewServer(etcd *etcdv3.Client, cfg ServerCfg, g Grid) (*Server, error) {
	setServerCfgDefaults(&cfg)

	if !isNameValid(cfg.Namespace) {
		return nil, ErrInvalidNamespace
	}
	if etcd == nil {
		return nil, ErrInvalidEtcd
	}
	return &Server{
		g:         g,
		cfg:       cfg,
		etcd:      etcd,
		grpc:      grpc.NewServer(),
		mailboxes: make(map[string]*Mailbox),
	}, nil
}

// Serve the grid on the listener. The listener address type must be
// net.TCPAddr, otherwise an error will be returned.
func (s *Server) Serve(lis net.Listener) error {
	r, err := registry.New(s.etcd)
	if err != nil {
		return err
	}
	s.registry = r
	s.registry.Timeout = s.cfg.Timeout
	s.registry.LeaseDuration = s.cfg.LeaseDuration

	addr, err := formatAddress(lis.Addr())
	if err != nil {
		return err
	}
	s.registry.Address = addr
	regFaults, err := s.registry.Start()
	if err != nil {
		return err
	}

	client, err := NewClient(s.etcd, ClientCfg{
		Namespace: s.cfg.Namespace,
		Timeout:   s.cfg.Timeout,
	})
	if err != nil {
		return err
	}
	s.client = client

	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, contextKey, &contextVal{
		server: s,
	})
	s.ctx = ctx
	s.cancel = cancel

	name := s.registry.Address
	name = strings.Replace(name, ":", "-", -1)
	name = strings.Replace(name, ".", "-", -1)
	name = strings.Replace(name, "/", "-", -1)
	name = strings.Trim(name, "~\\!@#$%^&*()<>")
	name = strings.TrimSpace(name)

	nsName, err := namespaceName(Peers, s.cfg.Namespace, name)
	if err != nil {
		return err
	}

	timeoutC, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	err = s.registry.Register(timeoutC, nsName)
	cancel()
	if err != nil {
		return err
	}

	mailbox, err := NewMailbox(s, name, 10)
	if err != nil {
		return err
	}
	go s.runMailbox(mailbox)

	runtimeErrors := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
		case err := <-regFaults:
			if err == nil {
				return
			}
			select {
			case runtimeErrors <- err:
			default:
			}
			s.Stop()
		}
	}()
	go func() {
		if s.g == nil || s.cfg.DisalowLeadership {
			return
		}
		def := NewActorDef("leader")
		var err error
		for i := 0; i < 6; i++ {
			time.Sleep(1 * time.Second)
			err = s.startActor(s.cfg.Timeout, def)
			if err == ErrGridReturnedNilActor {
				if Logger != nil {
					Logger.Printf("skipping leader startup since leader definition returned nil actor")
				}
				return
			}
			if err == nil || (err != nil && strings.Contains(err.Error(), "already registered")) {
				return
			}
		}
		select {
		case runtimeErrors <- fmt.Errorf("leader start failed: %v", err):
		default:
		}
		s.Stop()
	}()

	RegisterWireServer(s.grpc, s)
	err = s.grpc.Serve(lis)
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return err
	} else {
		err = nil
	}

	select {
	case err = <-runtimeErrors:
	default:
	}

	return err
}

// Stop the server, blocking until all mailboxes registered with
// this server have called their close method.
func (s *Server) Stop() {
	s.cancel()

	zeroMailboxes := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.mailboxes) == 0
	}

	t0 := time.Now()
	for {
		time.Sleep(200 * time.Millisecond)
		if zeroMailboxes() {
			break
		}
		if Logger != nil && time.Now().Sub(t0) > 10*time.Second {
			t0 = time.Now()
			for _, mailbox := range s.mailboxes {
				Logger.Printf("%v: waiting for mailbox to close: %v", s.cfg.Namespace, mailbox)
			}
		}
	}

	s.registry.Stop()
	s.grpc.Stop()
}

// Process a request and return a response. Implements the interface for
// gRPC definition of the wire service. Consider this a private method.
func (s *Server) Process(c netcontext.Context, d *Delivery) (*Delivery, error) {
	getMailbox := func() (*Mailbox, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		m, ok := s.mailboxes[d.Receiver]
		return m, ok
	}

	mailbox, ok := getMailbox()
	if !ok {
		return nil, ErrUnknownMailbox
	}

	// Write the bytes of the request into the byte
	// buffer for decoding.
	var buf bytes.Buffer
	n, err := buf.Write(d.Data)
	if err != nil {
		return nil, err
	}
	if n != len(d.Data) {
		return nil, io.ErrUnexpectedEOF
	}

	// Decode the request into an actual
	// type.
	env := &envelope{}
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(env)
	if err != nil {
		return nil, err
	}
	// This actually converts between the "context" and
	// "golang.org/x/net/context" types of Context so
	// that method signatures are satisfied.
	req := &request{}
	req.msg = env.Msg
	req.context = context.WithValue(c, "", "")
	req.response = make(chan []byte)

	// Send the filled envelope to the actual
	// receiver. Also note that the receiver
	// can stop listenting when it wants, so
	// some defualt or timeout always needs
	// to exist here.
	select {
	case mailbox.c <- req:
	default:
		return nil, ErrReceiverBusy
	}

	// Wait for the receiver to send back a
	// reply, or the context to finish.
	select {
	case <-c.Done():
		return nil, ErrContextFinished
	case data := <-req.response:
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
		case req := <-mailbox.C:
			switch msg := req.Msg().(type) {
			case *ActorDef:
				err := s.startActorC(req.Context(), msg)
				if err != nil {
					req.Respond(&ResponseMsg{
						Succeeded: false,
						Error:     err.Error(),
					})
				} else {
					req.Respond(&ResponseMsg{
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
func (s *Server) startActor(timeout time.Duration, def *ActorDef) error {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.startActorC(timeoutC, def)
}

// startActorC in the current process. This method does not communicate with another
// system to choose where to run the actor. Calling this method will start the
// actor on the current host in the current process.
func (s *Server) startActorC(c context.Context, def *ActorDef) error {
	if s.g == nil {
		return ErrActorCreationNotSupported
	}

	if !isNameValid(def.Type) {
		return ErrInvalidActorType
	}
	if !isNameValid(def.Name) {
		return ErrInvalidActorName
	}

	nsName, err := namespaceName(Actors, s.cfg.Namespace, def.Name)
	if err != nil {
		return err
	}

	actor, err := s.g.MakeActor(def)
	if err != nil {
		return err
	}
	if actor == nil {
		return ErrGridReturnedNilActor
	}

	// Register the actor. This acts as a distributed mutex to
	// prevent an actor from starting twice on one system or
	// many systems.
	timeout, cancel := context.WithTimeout(c, s.cfg.Timeout)
	err = s.registry.Register(timeout, nsName)
	cancel()
	if err != nil {
		return err
	}

	// The actor's context contains its full id, it's name and the
	// full registration, which contains the actor's namespace.
	actorCtx := context.WithValue(s.ctx, contextKey, &contextVal{
		server:    s,
		actorID:   nsName,
		actorName: def.Name,
	})

	// Start the actor, unregister the actor in case of failure
	// and capture panics that the actor raises.
	go func() {
		defer func() {
			timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
			s.registry.Deregister(timeout, nsName)
			cancel()
		}()
		defer func() {
			if err := recover(); err != nil {
				if Logger != nil {
					stack := niceStack(debug.Stack())
					log.Printf("panic in namespace: %v, actor: %v, recovered from: %v, stack: %v",
						s.cfg.Namespace, def.Name, err, stack)
				}
			}
		}()
		actor.Act(actorCtx)
	}()

	return nil
}

// formatAddress as ip:port, since just calling String()
// on the address can return some funky formatting.
func formatAddress(addr net.Addr) (string, error) {
	switch addr := addr.(type) {
	default:
		return "", ErrUnknownNetAddressType
	case *net.TCPAddr:
		if addr.IP.IsUnspecified() {
			return "", ErrUnspecifiedNetAddressIP
		}
		return fmt.Sprintf("%v:%v", addr.IP, addr.Port), nil
	}
}

func isServerRunning(s *Server) bool {
	return !(s == nil || s.ctx == nil || s.client == nil || s.cancel == nil || s.registry == nil)
}
