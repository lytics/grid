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

// EventType categorizing the event.
type EventType int

const (
	WatchError  EventType = 0
	EntityLost  EventType = 1
	EntityFound EventType = 2
)

// QueryEvent indicating that an entity has been discovered,
// lost, or some error has occured with the watch.
type QueryEvent struct {
	name   string
	peer   string
	err    error
	entity entityType
	Type   EventType
}

// Name of entity that caused the event.
func (e *QueryEvent) Name() string {
	return e.name
}

// Peer of named entity. If the entity is of type peer
// then methods Name and Peer return the same string.
func (e *QueryEvent) Peer() string {
	return e.peer
}

// Err caught watching query events. The error
// is not associated with any particular entity.
func (e *QueryEvent) Err() error {
	return e.err
}

// String representation of query event.
func (e *QueryEvent) String() string {
	if e == nil {
		return "query event: <nil>"
	}
	switch e.Type {
	case EntityLost:
		return fmt.Sprintf("query event: %v lost: %v", e.entity, e.name)
	case EntityFound:
		return fmt.Sprintf("query event: %v found: %v", e.entity, e.name)
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
//         switch event.Type {
//         case grid.WatchError:
//             // Error occured watching peers, deal with error.
//         case grid.EntityLost:
//             // Existing peer lost, reschedule work on extant peers.
//         case grid.EntityFound:
//             // New peer found, assign work, get data, reschedule, etc.
//         }
//     }
func (c *Client) QueryWatch(ctx context.Context, filter entityType) ([]*QueryEvent, <-chan *QueryEvent, error) {
	nsName, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, nil, err
	}

	regs, changes, err := c.registry.Watch(ctx, nsName)
	var current []*QueryEvent
	for _, reg := range regs {
		current = append(current, &QueryEvent{
			name:   nameFromReg(filter, c.cfg.Namespace, reg),
			peer:   reg.Name,
			entity: filter,
			Type:   EntityFound,
		})
	}

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
					put(&QueryEvent{err: change.Error})
					return
				}
				switch change.Type {
				case registry.Delete:
					put(&QueryEvent{
						name:   nameFromReg(filter, c.cfg.Namespace, change.Reg),
						peer:   change.Reg.Name,
						entity: filter,
						Type:   EntityLost,
					})
				case registry.Create, registry.Modify:
					put(&QueryEvent{
						name:   nameFromReg(filter, c.cfg.Namespace, change.Reg),
						peer:   change.Reg.Name,
						entity: filter,
						Type:   EntityFound,
					})
				}
			}
		}
	}()

	return current, queryEvents, nil
}

// Query in this client's namespace. The filter can be any one of
// Peers, Actors, or Mailboxes.
func (c *Client) Query(timeout time.Duration, filter entityType) ([]*QueryEvent, error) {
	timeoutC, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.QueryC(timeoutC, filter)
}

// QueryC (query) in this client's namespace. The filter can be any
// one of Peers, Actors, or Mailboxes. The context can be used to
// control cancelation or timeouts.
func (c *Client) QueryC(ctx context.Context, filter entityType) ([]*QueryEvent, error) {
	nsPrefix, err := namespacePrefix(filter, c.cfg.Namespace)
	if err != nil {
		return nil, err
	}
	regs, err := c.registry.FindRegistrations(ctx, nsPrefix)
	if err != nil {
		return nil, err
	}

	var result []*QueryEvent
	for _, reg := range regs {
		result = append(result, &QueryEvent{
			name:   nameFromReg(filter, c.cfg.Namespace, reg),
			peer:   reg.Name,
			entity: filter,
			Type:   EntityFound,
		})
	}

	return result, nil
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
