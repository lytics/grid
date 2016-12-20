package grid

import "context"

// Mailbox for receiving messages.
type Mailbox struct {
	name    string
	C       <-chan Request
	c       chan Request
	cleanup func() error
}

// Close the mailbox.
func (box *Mailbox) Close() error {
	return box.cleanup()
}

// String of mailbox name.
func (box *Mailbox) String() string {
	return box.name
}

// NewMailbox for requests addressed to name. Size will be the mailbox's
// channel size.
func NewMailbox(c context.Context, name string, size int) (*Mailbox, error) {
	namespace, err := ContextNamespace(c)
	if err != nil {
		return nil, err
	}

	s, err := contextServer(c)
	if err != nil {
		return nil, err
	}

	return NewMailboxWith(s, namespace, name, size)
}

// NewMailboxWith the server and namespace given explicitly rather than
// from a context. If creating a new mailbox from within the Act method
// of an actor, use NewMailbox instead, it's easier.
func NewMailboxWith(s *Server, namespace, name string, size int) (*Mailbox, error) {
	if !isServerRunning(s) {
		return nil, ErrServerNotRunning
	}
	if !isNameValid(name) {
		return nil, ErrInvalidMailboxName
	}

	// Namespaced name.
	nsName := namespace + "-" + name

	_, ok := s.mailboxes[nsName]
	if ok {
		return nil, ErrAlreadyRegistered
	}

	timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	err := s.registry.Register(timeout, nsName)
	cancel()
	if err != nil {
		return nil, err
	}

	cleanup := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Immediately delete the subscription so that no one
		// can send to it, at least from this host.
		delete(s.mailboxes, nsName)

		// Deregister the name.
		timeout, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
		err := s.registry.Deregister(timeout, nsName)
		cancel()

		// Return any error from the deregister call.
		return err
	}
	boxC := make(chan Request, size)
	box := &Mailbox{
		name:    nsName,
		C:       boxC,
		c:       boxC,
		cleanup: cleanup,
	}
	s.mailboxes[nsName] = box
	return box, nil
}
