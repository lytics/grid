package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lytics/retry"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

// Logger hides the logging function Printf behind a simple
// interface so libraries such as logrus can be used.
// Copied from package grid to avoid interndependencies.
type Logger interface {
	Printf(string, ...interface{})
}

var (
	ErrNotOwner                    = errors.New("registry: not owner")
	ErrNotStarted                  = errors.New("registry: not started")
	ErrUnknownKey                  = errors.New("registry: unknown key")
	ErrNilEtcd                     = errors.New("registry: nil etcd")
	ErrAlreadyRegistered           = errors.New("registry: already registered")
	ErrFailedRegistration          = errors.New("registry: failed registration")
	ErrFailedDeregistration        = errors.New("registry: failed deregistration")
	ErrLeaseDurationTooShort       = errors.New("registry: lease duration too short")
	ErrUnknownNetAddressType       = errors.New("registry: unknown net address type")
	ErrWatchClosedUnexpectedly     = errors.New("registry: watch closed unexpectedly")
	ErrUnspecifiedNetAddressIP     = errors.New("registry: unspecified net address ip")
	ErrKeepAliveClosedUnexpectedly = errors.New("registry: keep alive closed unexpectedly")
	ErrFailedAcquireAddressLock    = errors.New("registry: address lock exists after timeout")
)

var (
	minLeaseDuration = 10 * time.Second
)

// Registration information.
type Registration struct {
	Key         string   `json:"key"`
	Address     string   `json:"address"`
	Registry    string   `json:"registry"`
	Annotations []string `json:"annotations"`
}

// String descritpion of registration.
func (r *Registration) String() string {
	sort.Strings(r.Annotations)
	return fmt.Sprintf("key: %v, address: %v, registry: %v, annotations: %v",
		r.Key, r.Address, r.Registry, strings.Join(r.Annotations, ","))
}

// EventType of a watch event.
type EventType int

const (
	Error  EventType = 0
	Delete EventType = 1
	Modify EventType = 2
	Create EventType = 3
)

// WatchEvent triggred by a change in the registry.
type WatchEvent struct {
	Key   string
	Reg   *Registration
	Type  EventType
	Error error
}

// String representation of the watch event.
func (we *WatchEvent) String() string {
	if we.Error != nil {
		return fmt.Sprintf("key: %v, error: %v", we.Key, we.Error)
	}
	typ := "delete"
	switch we.Type {
	case Modify:
		typ = "modify"
	case Create:
		typ = "create"
	}
	return fmt.Sprintf("key: %v, type: %v, registration: %v", we.Key, typ, we.Reg)
}

// Registry for discovery.
type Registry struct {
	mu            sync.RWMutex
	started       bool
	done          chan bool
	exited        chan bool
	kv            etcdv3.KV
	lease         etcdv3.Lease
	leaseID       etcdv3.LeaseID
	client        *etcdv3.Client
	name          string
	address       string
	Logger        Logger
	Timeout       time.Duration
	LeaseDuration time.Duration
	// Testing hook.
	keepAliveStats *keepAliveStats
}

// New Registry.
func New(client *etcdv3.Client) (*Registry, error) {
	if client == nil {
		return nil, ErrNilEtcd
	}
	return &Registry{
		done:          make(chan bool),
		exited:        make(chan bool),
		kv:            etcdv3.NewKV(client),
		leaseID:       -1,
		client:        client,
		Timeout:       10 * time.Second,
		LeaseDuration: 60 * time.Second,
	}, nil
}

func registryLockKey(address string) string {
	return fmt.Sprintf("%v.%v", "registry.uniq-lock", address)
}

func (rr *Registry) waitForAddress(ctx context.Context, address string) error {
	key := registryLockKey(address)

	const infiniteRetries = 10000
	var locked bool = true
	var rErr error
	retry.X(infiniteRetries, time.Second, func() bool {
		select {
		case <-ctx.Done():
			// we timed out waiting for it to expire
			return false
		default:
		}

		res, err := rr.kv.Get(ctx, key, etcdv3.WithLimit(1))
		if err != nil {
			// don't retry on an error to etcd, just expect the process/pod to restart
			rErr = err
			return false
		}
		if res.Count != 0 {
			// the lock is being held by someone
			return true
		}
		// the lock is cleared
		locked = false
		return false
	})

	if rErr != nil && rErr != context.Canceled {
		return fmt.Errorf("registry: failed get address lock: error: %w", rErr)
	}

	if locked {
		return ErrFailedAcquireAddressLock
	}

	tctx, cancel := context.WithTimeout(ctx, rr.Timeout)
	_, err := rr.kv.Put(tctx, key, "", etcdv3.WithLease(rr.leaseID))
	cancel()
	if err != nil {
		return fmt.Errorf("registry: failed to write address lock: error: %w", err)
	}

	return nil
}

// Start Registry.
func (rr *Registry) Start(ctx context.Context, addr net.Addr) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	address, err := formatAddress(addr)
	if err != nil {
		return err
	}
	rr.address = address
	rr.name = formatName(address)

	if rr.LeaseDuration < minLeaseDuration {
		return ErrLeaseDurationTooShort
	}
	rr.lease = etcdv3.NewLease(rr.client)

	rctx, cancel := context.WithTimeout(ctx, rr.Timeout)
	defer cancel()
	res, err := rr.lease.Grant(rctx, int64(rr.LeaseDuration.Seconds()))
	if err != nil {
		return err
	}
	rr.leaseID = res.ID

	// Ensure that we're the owner of the address by taking an etcd lock
	rctx, cancel = context.WithTimeout(ctx, 2*rr.LeaseDuration) // retry until Lease is up...
	defer cancel()
	if err := rr.waitForAddress(rctx, address); err != nil {
		return err
	}

	// Start the keep alive for the lease.
	keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
	keepAlive, err := rr.lease.KeepAlive(keepAliveCtx, rr.leaseID)
	if err != nil {
		keepAliveCancel()
		return err
	}

	// There are two ways the Registry can exit:
	//     1) Someone calls Stop, in which case it will cancel
	//        its context and exit.
	//     2) The Registry fails to signal keep-alive on it
	//        lease repeatedly, in which case it will panic.
	go func() {
		defer close(rr.exited)

		// Track stats related to keep alive responses.
		var stats keepAliveStats
		defer func() {
			rr.keepAliveStats = &stats
		}()

		for {
			select {
			case <-rr.done:
				keepAliveCancel()
				rr.logf("registry: %v: keep alive closed", rr.name)
				return
			case res, open := <-keepAlive:
				if !open {
					// When the keep alive closes, check
					// if this was a close requested by
					// the user of the registry, or if
					// it was unexpected. If it was by
					// the user, the 'done' channel should
					// be closed.
					select {
					case <-rr.done:
						rr.logf("registry: %v: keep alive closed", rr.name)
						return
					default:
					}
					panic(fmt.Sprintf("registry: %v: keep alive closed unexpectedly", rr.name))
				}
				rr.logf("registry: %v: keep alive responded with heartbeat TTL: %vs", rr.name, res.TTL)
				stats.success++ // Testing hook
			}
		}
	}()

	rr.started = true
	return nil
}

// Address of this registry in the format of <ip>:<port>
func (rr *Registry) Address() string {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return rr.address
}

// Registry name, which is a human readable all ASCII
// transformation of the network address.
func (rr *Registry) Registry() string {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return rr.name
}

func (rr *Registry) Started() bool {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	return rr.started
}

// Stop Registry.
func (rr *Registry) Stop() error {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if rr.leaseID < 0 {
		return nil
	}
	// Close the done channel, to indicate
	// that this registry is done to its
	// background go-routines, such as the
	// keep-alive go-routine.
	select {
	case <-rr.done:
		// already done
		return nil
	default:
	}

	close(rr.done)
	// Wait for those background go-routines
	// to actually exit.
	<-rr.exited
	// Then revoke the lease to cleanly remove
	// all keys associated with this registry
	// from etcd.
	ctx, cancel := context.WithTimeout(context.Background(), rr.Timeout)
	defer cancel()
	if _, err := rr.lease.Revoke(ctx, rr.leaseID); err != nil {
		return err
	}

	if err := rr.lease.Close(); err != nil {
		return err
	}

	return nil
}

// Watch a prefix in the registry.
func (rr *Registry) Watch(c context.Context, prefix string) ([]*Registration, <-chan *WatchEvent, error) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	getRes, err := rr.kv.Get(c, prefix, etcdv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}
	registrations := make([]*Registration, 0, len(getRes.Kvs))
	for _, kv := range getRes.Kvs {
		reg := &Registration{}
		err = json.Unmarshal(kv.Value, reg)
		if err != nil {
			return nil, nil, err
		}
		registrations = append(registrations, reg)
	}

	// Channel to publish registry changes.
	watchEvents := make(chan *WatchEvent)

	// Write a change or exit the watcher.
	put := func(we *WatchEvent) {
		select {
		case <-c.Done():
			return
		case watchEvents <- we:
		}
	}
	putTerminalError := func(we *WatchEvent) {
		go func() {
			defer close(watchEvents)
			select {
			case <-time.After(10 * time.Minute):
			case watchEvents <- we:
			}
		}()
	}
	// Create a watch-event from an event.
	createWatchEvent := func(ev *etcdv3.Event) *WatchEvent {
		wev := &WatchEvent{Key: string(ev.Kv.Key)}
		reg := &Registration{}
		if ev.IsCreate() {
			wev.Type = Create
		} else if ev.IsModify() {
			wev.Type = Modify
		} else {
			wev.Type = Delete
			// Need to return now because
			// delete events don't contain
			// any data to unmarshal.
			return wev
		}
		err := json.Unmarshal(ev.Kv.Value, reg)
		if err != nil {
			wev.Error = fmt.Errorf("%v: failed unmarshaling value: '%s'", err, ev.Kv.Value)
		} else {
			wev.Reg = reg
		}
		return wev
	}

	// Watch deltas in etcd, with the give prefix, starting
	// at the revision of the get call above.
	deltas := rr.client.Watch(c, prefix, etcdv3.WithPrefix(), etcdv3.WithRev(getRes.Header.Revision+1))
	go func() {
		for delta := range deltas {
			if delta.Err() != nil {
				putTerminalError(&WatchEvent{Error: delta.Err()})
				return
			}
			for _, event := range delta.Events {
				put(createWatchEvent(event))
			}
		}

		select {
		case <-c.Done():
			close(watchEvents)
		default:
			putTerminalError(&WatchEvent{Error: ErrWatchClosedUnexpectedly})
		}
	}()

	return registrations, watchEvents, nil
}

// FindRegistrations associated with the prefix.
func (rr *Registry) FindRegistrations(c context.Context, prefix string) ([]*Registration, error) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	getRes, err := rr.kv.Get(c, prefix, etcdv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	registrations := make([]*Registration, 0, len(getRes.Kvs))
	for _, kv := range getRes.Kvs {
		reg := &Registration{}
		err = json.Unmarshal(kv.Value, reg)
		if err != nil {
			return nil, err
		}
		registrations = append(registrations, reg)
	}
	return registrations, nil
}

// FindRegistration associated with the given key.
func (rr *Registry) FindRegistration(c context.Context, key string) (*Registration, error) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	getRes, err := rr.kv.Get(c, key, etcdv3.WithLimit(1))
	if err != nil {
		return nil, err
	}
	if getRes.Count == 0 {
		return nil, ErrUnknownKey
	}
	reg := &Registration{}
	err = json.Unmarshal(getRes.Kvs[0].Value, reg)
	if err != nil {
		return nil, err
	}
	return reg, nil
}

// Register under the given key. A registration can happen only
// once, and registering more than once will return an error.
// Hence, registration can be used for mutual-exclusion.
func (rr *Registry) Register(c context.Context, key string, annotations ...string) error {
	sort.Strings(annotations)
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	if rr.leaseID < 0 {
		return ErrNotStarted
	}

	getRes, err := rr.kv.Get(c, key, etcdv3.WithLimit(1))
	if err != nil {
		return err
	}

	if getRes.Count > 0 {
		// The caller is regestering a key that is
		// already registered by another address.
		return ErrAlreadyRegistered
	}
	value, err := json.Marshal(&Registration{
		Key:         key,
		Address:     rr.address,
		Registry:    rr.name,
		Annotations: annotations,
	})
	if err != nil {
		return err
	}
	txnRes, err := rr.kv.Txn(c).
		If(etcdv3.Compare(etcdv3.Version(key), "=", 0)).
		Then(etcdv3.OpPut(key, string(value), etcdv3.WithLease(rr.leaseID))).
		Commit()
	if err != nil {
		return err
	}
	if !txnRes.Succeeded {
		return ErrFailedRegistration
	}
	return nil
}

// Deregister under the given key.
func (rr *Registry) Deregister(c context.Context, key string) error {
	rr.mu.RLock()
	defer rr.mu.RUnlock()

	if rr.leaseID < 0 {
		return ErrNotStarted
	}

	select {
	case <-rr.done:
		// Nothing to unregister, Registry is already
		// shutdown. "Deregistration" will be done by
		// Etcd deleting all keys associated with the
		// Registry's lease.
		return nil
	default:
	}

	getRes, err := rr.kv.Get(c, key, etcdv3.WithLimit(1))
	if err != nil {
		return err
	}
	if getRes.Count > 0 {
		kv := getRes.Kvs[0]
		rec := &Registration{}
		err = json.Unmarshal(kv.Value, rec)
		if err != nil {
			return err
		}
		if rec.Address != rr.address {
			return ErrNotOwner
		}

		txnRes, err := rr.kv.Txn(c).
			If(etcdv3.Compare(etcdv3.Version(key), "=", kv.Version)).
			Then(etcdv3.OpDelete(key)).
			Commit()
		if err != nil {
			return err
		}
		if !txnRes.Succeeded {
			return ErrFailedDeregistration
		}
	}
	return nil
}

func (rr *Registry) logf(format string, v ...interface{}) {
	if rr.Logger != nil {
		rr.Logger.Printf(format, v...)
	}
}

type keepAliveStats struct {
	success int
}

// formatName formats the address into a human readable form,
// removing any special characters.
func formatName(address string) string {
	name := address
	name = strings.Replace(name, ":", "-", -1)
	name = strings.Replace(name, ".", "-", -1)
	name = strings.Replace(name, "/", "-", -1)
	name = strings.Trim(name, "~\\!?@#$%^&*()<>+=|")
	name = strings.TrimSpace(name)
	return name
}

// formatAddress as ip:port, since just calling String()
// on the address can return some funky formatting.
func formatAddress(addr net.Addr) (string, error) {
	switch addr := addr.(type) {
	default:
		return "", ErrUnknownNetAddressType
	case *net.TCPAddr:
		if addr.IP.IsUnspecified() {
			return "", ErrUnspecifiedNetAddressIP
		}
		return fmt.Sprintf("%v:%v", addr.IP, addr.Port), nil
	}
}
