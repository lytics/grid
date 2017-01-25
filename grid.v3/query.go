package grid

import (
	"context"
	"fmt"
	"time"

	"github.com/lytics/grid/grid.v3/registry"
)

type entityType string

const (
	// Peers filter for query.
	Peers entityType = "peer"
	// Actors filter for query.
	Actors entityType = "actor"
	// Mailboxes filter for query.
	Mailboxes entityType = "mailbox"
)

type changeType int

const (
	entityErr        changeType = 0
	entityDiscovered changeType = 1
	entityLost       changeType = 2
)

// QueryEvent indicating that an entity has been discovered,
// lost, or some error has occured with the watch.
type QueryEvent struct {
	name   string
	err    error
	entity entityType
	change changeType
}

// Name of entity that caused the event.
func (e *QueryEvent) Name() string {
	return e.name
}

// Discovered entity.
func (e *QueryEvent) Discovered() bool {
	return e.change == entityDiscovered
}

// Lost entity.
func (e *QueryEvent) Lost() bool {
	return e.change == entityLost
}

// Err caught watching query events. The error
// is not associated with any particular entity.
func (e *QueryEvent) Err() error {
	return e.err
}

// String representation of query event.
func (e *QueryEvent) String() string {
	switch e.change {
	case entityLost:
		return fmt.Sprintf("query event: %v change: lost: %v", e.entity, e.name)
	case entityDiscovered:
		return fmt.Sprintf("query event: %v change: discovered: %v", e.entity, e.name)
	default:
		return fmt.Sprintf("query event: error: %v", e.err)
	}
}

// QueryWatch monitors the entry and exit of peers, actors, or mailboxes.
//
// Example usage:
//
//     client, err := grid.NewClient(...)
//     ...
//
//     currentpeers, watch, err := client.QueryWatch(c, grid.Peers)
//     ...
//
//     for _, peer := range currentpeers {
//         // Do work regarding peer.
//     }
//
//     for event := range watch {
//         if event.Err() != nil {
//             // Error occured watching peers, deal with error.
//         }
//         if event.Lost() {
//             // Existing peer lost, reschedule work on extant peers.
//         }
//         if event.Discovered() {
//             // New peer discovered, assign work, get data, reschedule, etc.
//         }
//     }
func (c *Client) QueryWatch(ctx context.Context, filter entityType) ([]string, <-chan *QueryEvent, error) {
	nsName, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, nil, err
	}

	regs, changes, err := c.registry.Watch(ctx, nsName)
	ents := nameFromRegs(filter, c.cfg.Namespace, regs)

	queryEvents := make(chan *QueryEvent)
	put := func(change *QueryEvent) {
		select {
		case <-ctx.Done():
		case queryEvents <- change:
		}
	}
	go func() {
		defer close(queryEvents)
		for {
			select {
			case <-ctx.Done():
				return
			case change, open := <-changes:
				if !open {
					put(&QueryEvent{err: ErrWatchClosedUnexpectedly})
					return
				}
				if change.Error != nil {
					put(&QueryEvent{err: err})
					return
				}
				switch change.Type {
				case registry.Delete:
					name := nameFromReg(filter, c.cfg.Namespace, change.Reg)
					put(&QueryEvent{name: name, entity: filter, change: entityLost})
				case registry.Create, registry.Modify:
					name := nameFromReg(filter, c.cfg.Namespace, change.Reg)
					put(&QueryEvent{name: name, entity: filter, change: entityDiscovered})
				}
			}
		}
	}()

	return ents, queryEvents, nil
}

// Query in this client's namespace. The filter can be any one of
// Peers, Actors, or Mailboxes.
func (c *Client) Query(timeout time.Duration, filter entityType) ([]string, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.QueryC(timeoutC, filter)
}

// QueryC (query) in this client's namespace. The filter can be any
// one of Peers, Actors, or Mailboxes. The context can be used to
// control cancelation or timeouts.
func (c *Client) QueryC(ctx context.Context, filter entityType) ([]string, error) {
	nsPrefix, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, err
	}
	regs, err := c.registry.FindRegistrations(ctx, nsPrefix)
	if err != nil {
		return nil, err
	}

	names := nameFromRegs(filter, c.cfg.Namespace, regs)
	return names, nil
}

// nameFromRegs returns a slice of names from many registrations.
// Used by query to return just simple string data.
func nameFromRegs(filter entityType, namespace string, regs []*registry.Registration) []string {
	names := make([]string, 0)
	for _, reg := range regs {
		name := nameFromReg(filter, namespace, reg)
		names = append(names, name)
	}
	return names
}

// nameFromReg returns the name from the data field of a registration.
// Used by query to return just simple string data.
func nameFromReg(filter entityType, namespace string, reg *registry.Registration) string {
	name, err := stripNamespace(filter, namespace, reg.Key)
	// INVARIANT
	// Under all circumstances if a registration is returned
	// from the prefix scan above, ie: FindRegistrations,
	// then each registration must contain the namespace
	// as a prefix of the key.
	if err != nil {
		panic("registry key without proper namespace prefix: " + reg.Key)
	}
	return name
}
