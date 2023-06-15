package grid

import (
	"errors"
	"sync"
)

var (
	// errDeregisteredFailed is used internally when we can't deregister a key from etcd.
	// It's used by Server.startActor() to ensure we panic.
	errDeregisteredFailed = errors.New("grid: deregistered failed")
)

// mailboxRegistry is a collection of named mailboxes.
type mailboxRegistry struct {
	mu sync.RWMutex
	r  map[string]*GRPCMailbox
}

func newMailboxRegistry() *mailboxRegistry {
	return &mailboxRegistry{
		r: make(map[string]*GRPCMailbox),
	}
}

// Get retrieves the mailbox.
func (r *mailboxRegistry) Get(name string) (m *GRPCMailbox, found bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, found = r.r[name]
	return
}

// Set the mailbox.
func (r *mailboxRegistry) Set(name string, m *GRPCMailbox) (update bool) {
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

// R returns a shallow copy of the underlying registry.
func (r *mailboxRegistry) R() map[string]*GRPCMailbox {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]*GRPCMailbox, len(r.r))
	for k, v := range r.r {
		out[k] = v
	}
	return out
}

type Mailbox interface {
	C() <-chan Request
	Close() error
}

// GRPCMailbox for receiving messages.
type GRPCMailbox struct {
	// mu protects c and closed
	mu     sync.RWMutex
	c      chan Request
	closed bool

	name     string
	nsName   string
	requests <-chan Request
	once     sync.Once
	cleanup  func()
}

func (box *GRPCMailbox) C() <-chan Request {
	return box.requests
}

// Close the mailbox.
func (box *GRPCMailbox) Close() error {
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
func (box *GRPCMailbox) Name() string {
	return box.name
}

// String of mailbox name, with full namespace.
func (box *GRPCMailbox) String() string {
	return box.nsName
}

// put a request into the mailbox if it is not closed,
// otherwise return an error indicating that the
// receiver is busy.
func (box *GRPCMailbox) put(req *request) error {
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
