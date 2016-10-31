package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

var Logger *log.Logger

type Option int

const (
	OpAllowReentrantRegistrations Option = 0
)

var (
	ErrNotOwner                    = errors.New("not owner")
	ErrNotStarted                  = errors.New("not started")
	ErrInvalidAddress              = errors.New("invalid address")
	ErrUnknownKey                  = errors.New("unknown address")
	ErrFailedRegistration          = errors.New("failed registration")
	ErrFailedDeregistration        = errors.New("failed deregistration")
	ErrAlreadyRegistered           = errors.New("already registered")
	ErrAlreadyDeregistered         = errors.New("already deregistered")
	ErrLeaseDurationTooShort       = errors.New("lease duration too short")
	ErrInvalidRegistrationRecord   = errors.New("invalid registration record")
	ErrMultipleRegistrationRecords = errors.New("multiple registration records")
)

var (
	minLeaseDuration           = 10 * time.Second
	heartbeatsPerLeaseDuration = 6
)

// Registration information.
type Registration struct {
	Key     string `json:"key"`
	Address string `json:"address"`
}

// Coordinator for discovery of receivers' host registrations.
type Coordinator struct {
	mu            sync.Mutex
	done          chan bool
	exited        chan bool
	kv            etcdv3.KV
	lease         etcdv3.Lease
	leaseID       etcdv3.LeaseID
	client        *etcdv3.Client
	address       string
	context       context.Context
	Timeout       time.Duration
	LeaseDuration time.Duration
	// Testing hook.
	keepAliveStats *keepAliveStats
}

// New coordinator.
func New(address string, client *etcdv3.Client) (*Coordinator, error) {
	if address == "" {
		return nil, ErrInvalidAddress
	}
	return &Coordinator{
		done:          make(chan bool),
		exited:        make(chan bool),
		kv:            etcdv3.NewKV(client),
		lease:         etcdv3.NewLease(client),
		leaseID:       -1,
		client:        client,
		address:       address,
		Timeout:       10 * time.Second,
		LeaseDuration: 60 * time.Second,
	}, nil
}

// StartHeartbeat of coordinator. The returned context should be used by
// everyone associated with this coordinator, and they should "shutdown"
// if the context signals done.
func (co *Coordinator) StartHeartbeat() error {
	co.mu.Lock()
	defer co.mu.Unlock()

	if co.LeaseDuration < minLeaseDuration {
		return ErrLeaseDurationTooShort
	}

	timeout, cancel := context.WithTimeout(context.Background(), co.Timeout)
	res, err := co.lease.Grant(timeout, int64(co.LeaseDuration.Seconds()))
	cancel()
	if err != nil {
		return err
	}
	co.leaseID = res.ID

	// Everyone using the coordinator for discovery will use
	// the context below to monitor its Done channel. If
	// the coordinator closes all users of the coordinator
	// will get notified.
	c, finalize := context.WithCancel(context.Background())
	co.context = c

	// There are two ways the coordinator can exit:
	//     1) Someone calls StopHeartbeat, in which case it will
	//        cancel its context and exit.
	//     2) The coordinator fails to signal keep-alive on it
	//        lease repeatedly, in which case it will cancel
	//        its context and exit.
	go func() {
		ticker := time.NewTicker(co.LeaseDuration / time.Duration(heartbeatsPerLeaseDuration))
		defer ticker.Stop()
		defer close(co.exited)

		stats := &keepAliveStats{}
		defer func() {
			co.keepAliveStats = stats
		}()

		errCnt := 0
		for {
			select {
			case <-co.done:
				timeout, cancel := context.WithTimeout(context.Background(), co.Timeout)
				co.lease.Revoke(timeout, co.leaseID)
				cancel()
				// Finialize, ie: DIE.
				finalize()
				return
			case <-ticker.C:
				timeout, cancel := context.WithTimeout(context.Background(), co.Timeout)
				_, err := co.lease.KeepAliveOnce(timeout, co.leaseID)
				cancel()
				if err != nil {
					stats.failure++
					errCnt++
					logPrintf("failed keep-alive heartbeat, total error count: %v, current error: %v", errCnt, err)
				} else {
					stats.success++
					errCnt = 0
				}
				if errCnt < heartbeatsPerLeaseDuration-1 {
					continue
				}
				// Finialize, ie: DIE.
				finalize()
				return
			}
		}
	}()

	return nil
}

// StopHeartbeat of coordinator.
func (co *Coordinator) StopHeartbeat() {
	if co.leaseID < 0 {
		return
	}
	close(co.done)
	<-co.exited
}

// Context of the coordinator. When the coordinator is stopped
// or fails, the context will signal done. Depending on the
// user's use of the coordinator, the user may also need to
// exit if it cannot function correctly without the use of
// the coordinator.
func (co *Coordinator) Context() context.Context {
	return co.context
}

// FindRegistrations associated with the prefix.
func (co *Coordinator) FindRegistrations(c context.Context, prefix string) ([]*Registration, error) {
	co.mu.Lock()
	defer co.mu.Unlock()

	getRes, err := co.kv.Get(c, prefix, etcdv3.WithPrefix())
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
func (co *Coordinator) FindRegistration(c context.Context, key string) (*Registration, error) {
	co.mu.Lock()
	defer co.mu.Unlock()

	getRes, err := co.kv.Get(c, key, etcdv3.WithLimit(1))
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
func (co *Coordinator) Register(c context.Context, key string, options ...Option) error {
	co.mu.Lock()
	defer co.mu.Unlock()

	if co.leaseID < 0 {
		return ErrNotStarted
	}

	getRes, err := co.kv.Get(c, key, etcdv3.WithLimit(1))
	if err != nil {
		return err
	}

	if getRes.Count > 0 {
		kv := getRes.Kvs[0]
		// The keys mach, so check if the caller has
		// allowed multiple registrations from the
		// same address.
		if len(options) == 0 {
			return ErrAlreadyRegistered
		}
		// The call HAS allowed multiple registrations
		// from the same address, so check if the
		// found record has the correct address.
		reg := &Registration{}
		err = json.Unmarshal(kv.Value, reg)
		if err != nil {
			return err
		}
		// The caller is already registered and they
		// have allowed just multi-registration, so
		// return.
		if reg.Address == co.address {
			return nil
		}
		// The caller is regestering a key that is
		// already registered by another address.
		return ErrAlreadyRegistered
	}

	value, err := json.Marshal(&Registration{
		Key:     key,
		Address: co.address,
	})
	if err != nil {
		return err
	}
	txnRes, err := co.kv.Txn(c).
		If(etcdv3.Compare(etcdv3.Version(key), "=", 0)).
		Then(etcdv3.OpPut(key, string(value), etcdv3.WithLease(co.leaseID))).
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
func (co *Coordinator) Deregister(c context.Context, key string) error {
	co.mu.Lock()
	defer co.mu.Unlock()

	if co.leaseID < 0 {
		return ErrNotStarted
	}

	select {
	case <-co.done:
		// Nothing to unregister, coordinator is already
		// shutdown. "Deregistration" will be done by
		// Etcd deleting all keys associated with the
		// coordinator's lease.
		return nil
	default:
	}

	getRes, err := co.kv.Get(c, key, etcdv3.WithLimit(1))
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
		if rec.Address != co.address {
			return ErrNotOwner
		}

		txnRes, err := co.kv.Txn(c).
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

type keepAliveStats struct {
	success int
	failure int
}

func logPrintf(format string, v ...interface{}) {
	if Logger != nil {
		Logger.Printf(format, v...)
	}
}
