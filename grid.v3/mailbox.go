package grid

import (
	"context"
	"fmt"
	"net"
	"time"
)

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
	if !isNameValid(name) {
		return nil, ErrInvalidMailboxName
	}

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
	boxC := make(chan Request, size)
	box := &Mailbox{
		name:    name,
		C:       boxC,
		c:       boxC,
		cleanup: cleanup,
	}
	s.mailboxes[name] = box
	return box, nil
}

func CleanAddress(addr net.Addr) (string, error) {
	switch ad := addr.(type) {
	default:
		return "", fmt.Errorf("unexpected type %T", ad) // %T prints whatever type t has
	case *net.TCPAddr:
		cleanad := fmt.Sprintf("%v:%v", ad.IP, ad.Port)
		if ad.IP.IsUnspecified() {
			return "", fmt.Errorf("the IP or hostname can't be unspecified: value:%v", cleanad)
		}
		return cleanad, nil
	}
}
