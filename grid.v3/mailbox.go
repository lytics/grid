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
//
// Example Usage:
//
//     func Act(c context.Context) {
//         mailbox, err := NewMailbox(c, "incoming", 10)
//         ...
//         defer mailbox.Close()
//
//         for {
//             select {
//             case req := <-mailbox.C:
//                 // Do something with request, and then respond
//                 // or ack. A response or ack is required.
//                 switch m := req.Msg().(type) {
//                 case *HiMsg:
//                     req.Respond(&HelloMsg{...})
//                 }
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
	if !isServerRunning(s) {
		return nil, ErrServerNotRunning
	}
	if !isNameValid(name) {
		return nil, ErrInvalidMailboxName
	}

	// Acquire lock for safe mailbox updates.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Namespaced name.
	nsName := s.cfg.Namespace + "-" + name

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
		defer cancel()
		err := s.registry.Deregister(timeout, nsName)

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
