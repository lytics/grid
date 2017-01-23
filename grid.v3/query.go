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

// QueryEvent indicating that a peer has been discovered,
// lost, or some error has occured with a peer or the watch
// of peers.
type QueryEvent struct {
	name   string
	err    error
	entity entityType
	change changeType
}

// Name of entity that caused the event.
func (p *QueryEvent) Name() string {
	return p.name
}

// Discovered entity.
func (p *QueryEvent) Discovered() bool {
	return p.change == entityDiscovered
}

// Lost entity.
func (p *QueryEvent) Lost() bool {
	return p.change == entityLost
}

// Err caught watching peers. The error is not
// associated with any particular peer.
func (p *QueryEvent) Err() error {
	return p.err
}

// String representation of peer change.
func (p *QueryEvent) String() string {
	switch p.change {
	case entityLost:
		return fmt.Sprintf("query event: %v change: lost: %v", p.entity, p.name)
	case entityDiscovered:
		return fmt.Sprintf("query event: %v change: discovered: %v", p.entity, p.name)
	default:
		return fmt.Sprintf("query event: error: %v", p.err)
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
