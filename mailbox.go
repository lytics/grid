package grid

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lytics/grid/v3/registry"
	"github.com/lytics/retry"
)

var (
	// errDeregisteredFailed is used internally when we can't deregister a key from etcd.
	// It's used by Server.startActorC() to ensure we panic.
	errDeregisteredFailed = errors.New("grid: deregistered failed")
)

// mailboxRegistry is a collection of named mailboxes.
type mailboxRegistry struct {
	mu sync.RWMutex
	r  map[string]*Mailbox
}

func newMailboxRegistry() *mailboxRegistry {
	return &mailboxRegistry{
		r: make(map[string]*Mailbox),
	}
}

// Get retrieves the mailbox.
func (r *mailboxRegistry) Get(name string) (m *Mailbox, found bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, found = r.r[name]
	return
}

// Set the mailbox.
func (r *mailboxRegistry) Set(name string, m *Mailbox) (update bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, update = r.r[name]
	r.r[name] = m
	return
}

// Delete the mailbox.
func (r *mailboxRegistry) Delete(name string) (found bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, found = r.r[name]
	if found {
		delete(r.r, name)
	}
	return
}

// Size of the registry.
func (r *mailboxRegistry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.r)
}

// R returns the underlying registry.
// It does _not_ return a copy.
func (r *mailboxRegistry) R() map[string]*Mailbox {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.r
}

// Mailbox for receiving messages.
type Mailbox struct {
	// mu protects c and closed
	mu     sync.RWMutex
	c      chan Request
	closed bool

	name    string
	nsName  string
	C       <-chan Request
	once    sync.Once
	cleanup func()
}

// Close the mailbox.
func (box *Mailbox) Close() error {
	box.once.Do(func() {
		box.mu.Lock()
		close(box.c)
		box.closed = true
		box.mu.Unlock()
		// Run server-provided clean up.
		box.cleanup()
	})
	return nil
}

// Name of mailbox, without namespace.
func (box *Mailbox) Name() string {
	return box.name
}

// String of mailbox name, with full namespace.
func (box *Mailbox) String() string {
	return box.nsName
}

// put a request into the mailbox if it is not closed,
// otherwise return an error indicating that the
// receiver is busy.
func (box *Mailbox) put(req *request) error {
	// NOTE (2022-01) (mh): We have to defer the unlock here
	// as it's not safe otherwise.
	//
	// If we RUnlock() after reading box.closed:
	// goroutine 1 (put)  : RLock -  RUnlock() -   send (!!!)
	// goroutine 2 (close):       Lock - close - Unlock
	//
	// If we RUnlock() at the end:
	// goroutine 1 (put)  : RLock - send - RUnlock()
	// goroutine 2 (close):      Lock -              close - Unlock
	box.mu.RLock()
	defer box.mu.RUnlock()

	if box.closed {
		return ErrReceiverBusy
	}

	select {
	case box.c <- req:
		return nil
	default:
		return ErrReceiverBusy
	}
}

// NewMailbox for requests addressed to name. Size will be the mailbox's
// channel size.
//
// Example Usage:
//
//     mailbox, err := NewMailbox(server, "incoming", 10)
//     ...
//     defer mailbox.Close()
//
//     for {
//         select {
//         case req := <-mailbox.C:
//             // Do something with request, and then respond
//             // or ack. A response or ack is required.
//             switch m := req.Msg().(type) {
//             case HiMsg:
//                 req.Respond(&HelloMsg{})
//             }
//         }
//     }
//
// If the mailbox has already been created, in the calling process or
// any other process, an error is returned, since only one mailbox
// can claim a particular name.
//
// Using a mailbox requires that the process creating the mailbox also
// started a grid Server.
func NewMailbox(s *Server, name string, size int) (*Mailbox, error) {
	if !isNameValid(name) {
		return nil, fmt.Errorf("%w: name=%s", ErrInvalidMailboxName, name)
	}

	// Namespaced name.
	nsName, err := namespaceName(Mailboxes, s.cfg.Namespace, name)
	if err != nil {
		return nil, err
	}

	return newMailbox(s, name, nsName, size)
}

func newMailbox(s *Server, name, nsName string, size int) (*Mailbox, error) {
	if err := s.isServing(); err != nil {
		return nil, err
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
	box := &Mailbox{
		name:    name,
		nsName:  nsName,
		C:       boxC,
		c:       boxC,
		cleanup: cleanup,
	}
	s.mailboxes.Set(nsName, box)
	return box, nil
}
