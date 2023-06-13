package grid

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/lytics/grid/v3/codec"
	"github.com/lytics/grid/v3/registry"
	"github.com/lytics/retry"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	ctxKey contextKey = "grid-context-key-xboKEsHA26"
)

type contextKey string

type contextVal struct {
	server    *Server
	actorID   string
	actorName string
}

// Server of a grid.
type Server struct {
	UnimplementedWireServer
	// mu protects the follow fields, use accessors
	mu       sync.RWMutex
	finalErr error

	ctx       context.Context
	cancel    func()
	cfg       ServerCfg
	etcd      *etcdv3.Client
	grpc      grpcServer
	health    *health.Server
	stop      sync.Once
	fatalErr  chan error
	actors    *makeActorRegistry
	registry  *registry.Registry
	mailboxes *mailboxRegistry
	creds     credentials.TransportCredentials
}

type grpcServer interface {
	RegisterService(desc *grpc.ServiceDesc, impl interface{})
	Serve(lis net.Listener) error
	Stop()
}

// NewServer for the grid. The namespace must contain only characters
// in the set: [a-zA-Z0-9-_] and no other.
func NewServer(etcd *etcdv3.Client, cfg ServerCfg) (*Server, error) {
	setServerCfgDefaults(&cfg)

	if !isNameValid(cfg.Namespace) {
		return nil, fmt.Errorf("%w: namespace=%s", ErrInvalidNamespace, cfg.Namespace)
	}
	if etcd == nil {
		return nil, ErrNilEtcd
	}

	creds := insecure.NewCredentials()
	// Create a registry client, through which other
	// entities like peers, actors, and mailboxes
	// will be discovered.
	r, err := registry.New(etcd)
	if err != nil {
		return nil, err
	}
	r.Timeout = cfg.Timeout
	r.LeaseDuration = cfg.LeaseDuration

	// Set registry logger.
	if cfg.Logger != nil {
		r.Logger = cfg.Logger
	}

	ser := grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor()),
		grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor()),
	)

	server := &Server{
		cfg:       cfg,
		etcd:      etcd,
		grpc:      ser,
		health:    health.NewServer(),
		actors:    newMakeActorRegistry(),
		fatalErr:  make(chan error, 1),
		registry:  r,
		mailboxes: newMailboxRegistry(),
		creds:     creds,
	}

	// Create a context that each actor this leader creates
	// will receive. When the server is stopped, it will
	// call the cancel function, which should cause all the
	// actors it is responsible for to shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, ctxKey, &contextVal{
		server: server,
	})
	server.ctx = ctx
	server.cancel = cancel

	return server, nil
}

// NewMailbox for requests addressed to name. Size will be the mailbox's
// channel size.
//
// Example Usage:
//
//	mailbox, err := server.NewMailbox("incoming", 10)
//	...
//	defer mailbox.Close()
//
//	for {
//	    select {
//	    case req := <-mailbox.C:
//	        // Do something with request, and then respond
//	        // or ack. A response or ack is required.
//	        switch m := req.Msg().(type) {
//	        case HiMsg:
//	            req.Respond(&HelloMsg{})
//	        }
//	    }
//	}
//
// If the mailbox has already been created, in the calling process or
// any other process, an error is returned, since only one mailbox
// can claim a particular name.
//
// Using a mailbox requires that the process creating the mailbox also
// started a grid Server.
func (s *Server) NewMailbox(name string, size int) (Mailbox, error) {
	if !isNameValid(name) {
		return nil, fmt.Errorf("%w: name=%s", ErrInvalidMailboxName, name)
	}

	// Namespaced name.
	nsName, err := namespaceName(Mailboxes, s.cfg.Namespace, name)
	if err != nil {
		return nil, err
	}

	return s.newMailbox(name, nsName, size)
}

func (s *Server) newMailbox(name, nsName string, size int) (*GRPCMailbox, error) {
	if !s.registry.Started() {
		return nil, ErrServerNotRunning
	}

	_, ok := s.mailboxes.Get(nsName)
	if ok {
		return nil, fmt.Errorf("%w: nsName=%s", ErrAlreadyRegistered, nsName)
	}

	cleanup := func() {
		// Immediately delete the subscription so that no one
		// can send to it, at least from this host.
		s.mailboxes.Delete(nsName)

		// Deregister the name.
		var err error
		retry.X(3, 3*time.Second,
			func() bool {
				timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
				err = s.registry.Deregister(timeout, nsName)
				cancel()
				return err != nil
			})
		// Ignore ErrNotOwner because most likely the previous owner panic'ed or exited badly.
		// So we'll ignore the error and let the mailbox creator retry later.  We don't want to panic
		// in that case because it will take down more mailboxes and make it worse.
		if err != nil && err != registry.ErrNotOwner {
			panic(fmt.Errorf("%w: unable to deregister mailbox: %v, error: %v", errDeregisteredFailed, nsName, err))
		}
	}

	timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	err := s.registry.Register(timeout, nsName)
	cancel()
	// Check if the error is a particular fatal error
	// from etcd. Some errors have no recovery. See
	// the list of all possible errors here:
	//
	// https://github.com/etcd-io/etcd/blob/master/etcdserver/api/v3rpc/rpctypes/error.go
	//
	// They are unfortunately not classidied into
	// recoverable or non-recoverable.
	if err != nil && strings.Contains(err.Error(), "etcdserver: requested lease not found") {
		s.reportFatalError(err)
		return nil, err
	}
	if err != nil {
		cleanup()
		return nil, err
	}

	boxC := make(chan Request, size)
	box := &GRPCMailbox{
		name:     name,
		nsName:   nsName,
		requests: boxC,
		c:        boxC,
		cleanup:  cleanup,
	}
	s.mailboxes.Set(nsName, box)
	return box, nil
}

// RegisterDef of an actor. When a ActorStart message is sent to
// a peer it will use the registered definitions to make and run
// the actor. If an actor with actorType "leader" is registered
// it will be started automatically when the Serve method is
// called.
func (s *Server) RegisterDef(actorType string, f MakeActor) {
	s.actors.Set(actorType, f)
}

// Context of the server, when it reports done the
// server is trying to shutdown. Actors automatically
// get this context, non-actors using mailboxes bound
// to this server should monitor this context to know
// when the server is trying to exit.
func (s *Server) Context() context.Context {
	return s.ctx
}

// Name of the server. Only valid after Serve() is called and
// the registry has started (server's name is the registry's name).
// Use
func (s *Server) Name() string {
	return s.registry.Registry()
}

// Serve the grid on the listener. The listener address type must be
// net.TCPAddr, otherwise an error will be returned.
func (s *Server) Serve(lis net.Listener) error {
	if err := s.registry.Start(lis.Addr()); err != nil {
		return fmt.Errorf("starting registry: %w", err)
	}

	// Namespaced name, which just includes the namespace.
	nsName, err := namespaceName(Peers, s.cfg.Namespace, s.Name())
	if err != nil {
		return fmt.Errorf("namspacing %v: %w", s.Name(), err)
	}

	// Register the namespace name, other peers can search
	// for this to discover each other.
	timeoutC, cancel := context.WithTimeout(s.ctx, s.cfg.Timeout)
	err = s.registry.Register(timeoutC, nsName, s.cfg.Annotations...)
	cancel()
	if err != nil {
		return fmt.Errorf("registering: %w", err)
	}

	// Start a mailbox, this is critical because starting
	// actors in a grid is just done via a normal request
	// sending the message ActorDef to a listening peer's
	// mailbox.
	mailbox, err := s.NewMailbox(s.Name(), 100)
	if err != nil {
		return fmt.Errorf("creating mailbox: %w", err)

	}
	go s.runMailbox(mailbox)

	if !s.cfg.DisallowLeadership {
		// Start the leader actor, and monitor, ie: make sure
		// that it's running.
		s.monitorLeader()
	}

	// Monitor for fatal errors.
	s.monitorFatalErrors()

	// gRPC dance to start the gRPC server. The Serve
	// method blocks still stopped via a call to Stop.
	RegisterWireServer(s.grpc, s)
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s.grpc, s.health)

	err = s.grpc.Serve(lis)
	// Something in gRPC returns the "use of..." error
	// message even though it stopped fine. Catch that
	// error and don't pass it up.
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		return fmt.Errorf("serving: %w", err)
	}

	// Return the final error state, which
	// could be set by other locations.
	if err := s.getFinalErr(); err != nil {
		return fmt.Errorf("fatal error: %w", err)
	}
	return nil
}

// WaitUntilStarted waits until the registry has started or until the context is done.
// This allows users to safely access some runtime-specific parameters (e.g., Name()).
// There is no guarantee that the gRPC client has started: use Client.WaitUntilServing()
// for that.
func (s *Server) WaitUntilStarted(ctx context.Context) error {
	b := newBackoff()
	defer b.Stop()

	for {
		if err := b.Backoff(ctx); err != nil {
			return fmt.Errorf("backing off: %w", err)
		}
		if !s.registry.Started() {
			s.logf("not yet started")
			continue
		}
		return nil
	}
}

// Stop the server, blocking until all mailboxes registered with
// this server have called their close method.
func (s *Server) Stop() {
	s.stop.Do(func() {
		if s.cancel == nil {
			return
		}
		s.cancel()

		t0 := time.Now()
		for {
			time.Sleep(200 * time.Millisecond)
			if s.mailboxes.Size() == 0 {
				break
			}

			if time.Since(t0) > 20*time.Second {
				t0 = time.Now()
				for _, m := range s.mailboxes.R() {
					s.logf("%v: waiting for mailbox to close: %v", s.cfg.Namespace, m)
				}
			}
		}

		if err := s.registry.Stop(); err != nil {
			s.logf("%v: stopping registry: %v", s.cfg.Namespace, err)
		}
		s.grpc.Stop()
	})
}

// Process a request and return a response. Implements the interface for
// gRPC definition of the wire service. Consider this a private method.
func (s *Server) Process(c context.Context, d *Delivery) (*Delivery, error) {
	mailbox, ok := s.mailboxes.Get(d.Receiver)
	if !ok {
		return nil, ErrUnknownMailbox
	}

	// Decode the request into an actual msg.
	msg, err := codec.Unmarshal(d.Data, d.TypeName)
	if err != nil {
		return nil, err
	}

	req := newRequest(c, msg)

	// Send the filled envelope to the actual
	// receiver. Also note that the receiver
	// can stop listenting when it wants, so
	// the receiver may return an error saying
	// it is busy.
	err = mailbox.put(req)
	if err != nil {
		return nil, err
	}

	// Wait for the receiver to send back a
	// reply, or the context to finish.
	select {
	case <-c.Done():
		return nil, ErrContextFinished
	case fail := <-req.failure:
		return nil, fail
	case res := <-req.response:
		return res, nil
	}
}

// runMailbox for this server.
func (s *Server) runMailbox(mailbox Mailbox) {
	defer mailbox.Close()
	for {
		select {
		case <-s.ctx.Done():
			return
		case req := <-mailbox.C():
			switch msg := req.Msg().(type) {
			case *ActorStart:
				err := s.startActorC(req.Context(), msg)
				if err != nil {
					err2 := req.Respond(err)
					if err2 != nil {
						s.logf("%v: failed sending response for failed actor start: %v, original error: %v", s.cfg.Namespace, err2, err)
					}
				} else {
					err := req.Ack()
					if err != nil {
						s.logf("%v: failed sending ack: %v", s.cfg.Namespace, err)
					}
				}
			}
		}
	}
}

// monitorFatalErrors and stop the server if one occurs.
func (s *Server) monitorFatalErrors() {
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case err := <-s.fatalErr:
				if err != nil {
					s.putFinalErr(err)
					s.Stop()
				}
			}
		}
	}()
}

// monitorLeader starts a leader and keeps tyring to start
// a leader thereafter. If the leader should die on any
// host then some peer will eventually have it start again.
func (s *Server) monitorLeader() {
	startLeader := func() error {
		var err error
		for i := 0; i < 6; i++ {
			select {
			case <-s.ctx.Done():
				return nil
			default:
			}
			time.Sleep(1 * time.Second)
			err = s.startActor(s.cfg.Timeout, &ActorStart{Name: "leader", Type: "leader"})
			if err != nil && strings.Contains(err.Error(), registry.ErrAlreadyRegistered.Error()) {
				return nil
			}
		}
		return err
	}

	go func() {
		timer := time.NewTimer(0 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-timer.C:
				err := startLeader()
				if errors.Is(err, ErrDefNotRegistered) {
					s.logf("skipping leader startup since leader definition not registered")
					return
				}
				if errors.Is(err, ErrNilActor) {
					s.logf("skipping leader startup since make leader returned nil")
					return
				}
				if err != nil {
					s.reportFatalError(fmt.Errorf("leader start failed: %v", err))
				} else {
					timer.Reset(30 * time.Second)
				}
			}
		}
	}()
}

// reportFatalError to the fatal error monitor. The
// consequence of a fatal error is handled by the
// monitor itself.
func (s *Server) reportFatalError(err error) {
	if err != nil {
		select {
		case s.fatalErr <- err:
		default:
		}
	}
}

func (s *Server) getFinalErr() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.finalErr
}

func (s *Server) putFinalErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalErr = err
}

// startActor in the current process. This method does not communicate with another
// system to choose where to run the actor. Calling this method will start the
// actor on the current host in the current process.
func (s *Server) startActor(timeout time.Duration, start *ActorStart) error {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.startActorC(timeoutC, start)
}

// startActorC in the current process. This method does not communicate with another
// system to choose where to run the actor. Calling this method will start the
// actor on the current host in the current process.
func (s *Server) startActorC(c context.Context, start *ActorStart) error {
	if !isNameValid(start.Type) {
		return fmt.Errorf("%w: type=%s", ErrInvalidActorType, start.Type)
	}

	nsName, err := namespaceName(Actors, s.cfg.Namespace, start.Name)
	if err != nil {
		return err
	}

	makeActor, ok := s.actors.Get(start.Type)
	if !ok {
		return fmt.Errorf("%w: type=%s", ErrDefNotRegistered, start.Type)
	}
	if makeActor == nil {
		return fmt.Errorf("%w (nil def): type=%s", ErrDefNotRegistered, start.Type)
	}

	actor, err := makeActor(start.Data)
	if err != nil {
		return err
	}
	if actor == nil {
		return ErrNilActor
	}

	// Register the actor. This acts as a distributed mutex to
	// prevent an actor from starting twice on one system or
	// many systems.
	timeout, cancel := context.WithTimeout(c, s.cfg.Timeout)
	defer cancel()
	if err := s.registry.Register(timeout, nsName); err != nil {
		// Grid tries to start up leaders continuously so we need to ignore calling deregister
		// otherwise we will deregister a leader out from under itself starting multiple leaders
		if !(start.Type == "leader" && strings.Contains(err.Error(), registry.ErrAlreadyRegistered.Error())) {
			s.deregisterActor(nsName)
		}
		return fmt.Errorf("registering actor %q: %w", nsName, err)
	}

	// The actor's context contains its full id, it's name and the
	// full registration, which contains the actor's namespace.
	actorCtx := context.WithValue(s.ctx, ctxKey, &contextVal{
		server:    s,
		actorID:   nsName,
		actorName: start.Name,
	})

	// Start the actor, unregister the actor in case of failure
	// and capture panics that the actor raises.
	go func() {
		defer s.deregisterActor(nsName)
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok && errors.Is(err, errDeregisteredFailed) {
					// NOTE (2021-06) (mh): We need to panic here
					panic(err)
				}

				stack := niceStack(debug.Stack())
				s.logf("panic in namespace: %v, actor: %v, recovered from: %v, stack trace: %v",
					s.cfg.Namespace, start.Name, r, stack)
			}
		}()
		actor.Act(actorCtx)
	}()

	return nil
}

func (s *Server) deregisterActor(nsName string) {
	var err error
	retry.X(3, 3*time.Second,
		func() bool {
			timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
			err = s.registry.Deregister(timeout, nsName)
			cancel()
			return err != nil
		})
	// Ignore ErrNotOwner because most likely the previous owner panic'ed or exited badly.
	// So we'll ignore the error and let the mailbox creator retry later.  We don't want to panic
	// in that case because it will take down more mailboxes and make it worse.
	if err != nil && err != registry.ErrNotOwner {
		panic(fmt.Sprintf("unable to deregister actor: %v, error: %v", nsName, err))
	}
}

func (s *Server) logf(format string, v ...interface{}) {
	if s.cfg.Logger != nil {
		s.cfg.Logger.Printf(format, v...)
	}
}
