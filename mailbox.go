package grid

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lytics/retry"
)

var (
	// errDeregisteredFailed is used internally when we can't deregister a key from etcd.
	// It's used by Server.startActorC() to ensure we panic.
	errDeregisteredFailed = errors.New("grid: deregistered failed")
)

// Mailbox for receiving messages.
type Mailbox struct {
	mu      sync.RWMutex
	name    string
	nsName  string
	C       <-chan Request
	c       chan Request
	closed  bool
	cleanup func()
}

// Close the mailbox.
func (box *Mailbox) Close() error {
	box.mu.Lock()
	defer box.mu.Unlock()

	// Close mailbox.
	box.closed = true
	close(box.c)

	box.cleanup()
	// Run server provided clean up.
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
	s.mu.Lock()
	if s.mailboxes == nil {
		s.mu.Unlock()
		return nil, ErrServerNotRunning
	}
	s.mu.Unlock()

	s.mumb.Lock()
	defer s.mumb.Unlock()

	_, ok := s.mailboxes[nsName]
	if ok {
		return nil, fmt.Errorf("%w: nsName=%s", ErrAlreadyRegistered, nsName)
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
		return nil, err
	}

	boxC := make(chan Request, size)
	cleanup := func() {
		s.mumb.Lock()
		defer s.mumb.Unlock()

		// Immediately delete the subscription so that no one
		// can send to it, at least from this host.
		delete(s.mailboxes, nsName)

		// Deregister the name.
		var err error
		retry.X(3, 3*time.Second,
			func() bool {
				timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
				err = s.registry.Deregister(timeout, nsName)
				cancel()
				return err != nil
			})
		if err != nil {
			panic(fmt.Errorf("%w: unable to deregister mailbox: %v, error: %v", errDeregisteredFailed, nsName, err))
		}
	}
	box := &Mailbox{
		name:    name,
		nsName:  nsName,
		C:       boxC,
		c:       boxC,
		cleanup: cleanup,
	}
	s.mailboxes[nsName] = box
	return box, nil
}
