package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

const keepAlivesPerLeaseDuration = 6

var (
	ErrInvalidAddress            = errors.New("invalid address")
	ErrUnknownAddress            = errors.New("unknown address")
	ErrInvalidRegistration       = errors.New("invalid registration")
	ErrAlreadyDeregistered       = errors.New("already deregistered")
	ErrInvalidDeregistration     = errors.New("invalid deregistration")
	ErrAlreadyRegistered         = errors.New("receiver already exists")
	ErrInvalidReceiverOwnership  = errors.New("invalid receiver ownership")
	ErrInvalidRegistrationRecord = errors.New("invalid registration record set")
)

type regRec struct {
	Address string `json:"address"`
}

// Coordinator for discovery of receivers' host addresses.
type Coordinator struct {
	mu            sync.Mutex
	done          chan bool
	kv            etcdv3.KV
	lease         etcdv3.Lease
	leaseID       etcdv3.LeaseID
	client        *etcdv3.Client
	address       string
	addresses     map[string]string
	context       context.Context
	Timeout       time.Duration
	LeaseDuration time.Duration
}

func New(address string, client *etcdv3.Client) (*Coordinator, error) {
	if address == "" {
		return nil, ErrInvalidAddress
	}
	return &Coordinator{
		done:          make(chan bool),
		kv:            etcdv3.NewKV(client),
		lease:         etcdv3.NewLease(client),
		client:        client,
		address:       address,
		addresses:     make(map[string]string),
		Timeout:       10 * time.Second,
		LeaseDuration: 60 * time.Second,
	}, nil
}

// StartHeartbeat. The returned context should be used by everyone
// associated with this coordinator, and they should "shutdown"
// if the context is "done".
func (co *Coordinator) StartHeartbeat() error {
	co.mu.Lock()
	defer co.mu.Unlock()

	timeout, cancel := context.WithTimeout(context.Background(), co.Timeout)
	res, err := co.lease.Grant(timeout, int64(co.LeaseDuration.Seconds()))
	cancel()
	if err != nil {
		return err
	}
	co.leaseID = res.ID

	c, finalize := context.WithCancel(context.Background())
	co.context = c

	go func() {
		ticker := time.NewTicker(co.LeaseDuration / keepAlivesPerLeaseDuration)
		defer ticker.Stop()
		errCnt := 0
		for {
			select {
			case <-co.done:
				timeout, cancel := context.WithTimeout(context.Background(), co.Timeout)
				co.lease.Revoke(timeout, co.leaseID)
				cancel()
				co.client.Close()
				// Finialize, ie: DIE.
				finalize()
				return
			case <-ticker.C:
				timeout, cancel := context.WithTimeout(context.Background(), co.Timeout)
				_, err := co.lease.KeepAliveOnce(timeout, co.leaseID)
				cancel()
				if err != nil {
					errCnt++
				}
				if errCnt < keepAlivesPerLeaseDuration-1 {
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

// StopHeartbeat.
func (co *Coordinator) StopHeartbeat() {
	close(co.done)
}

func (co *Coordinator) Context() context.Context {
	return co.context
}

// FindAddresses associated with the prefix.
func (co *Coordinator) FindAddresses(c context.Context, prefix string) ([][]string, error) {
	co.mu.Lock()
	defer co.mu.Unlock()

	res, err := co.kv.Get(c, prefix)
	if err != nil {
		return nil, err
	}
	addresses := make([][]string, len(res.Kvs))
	for _, kv := range res.Kvs {
		rec := &regRec{}
		err = json.Unmarshal(kv.Value, rec)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, []string{string(kv.Key), rec.Address})
	}
	return addresses, nil
}

// FindAddress associated with the given key.
func (co *Coordinator) FindAddress(c context.Context, key string) (string, error) {
	co.mu.Lock()
	defer co.mu.Unlock()

	address, ok := co.addresses[key]
	if ok {
		return address, nil
	}

	res, err := co.kv.Get(c, key)
	if err != nil {
		return "", err
	}
	if res == nil || len(res.Kvs) == 0 {
		return "", ErrUnknownAddress
	}
	if len(res.Kvs) > 1 {
		return "", ErrInvalidRegistration
	}
	rec := &regRec{}
	err = json.Unmarshal(res.Kvs[0].Value, rec)
	if err != nil {
		return "", err
	}
	co.addresses[key] = rec.Address
	return rec.Address, nil
}

// Register under the given key.
func (co *Coordinator) Register(c context.Context, key string) error {
	co.mu.Lock()
	defer co.mu.Unlock()

	getRes, err := co.kv.Get(c, key)
	if err != nil {
		return err
	}
	if getRes != nil && len(getRes.Kvs) > 1 {
		return ErrInvalidRegistrationRecord
	}
	if getRes != nil && len(getRes.Kvs) == 1 {
		rec := &regRec{}
		err = json.Unmarshal(getRes.Kvs[0].Value, rec)
		if err != nil {
			return err
		}
		if rec.Address == co.address {
			return nil
		} else {
			return ErrAlreadyRegistered
		}
	}

	value, err := json.Marshal(&regRec{Address: co.address})
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
		return ErrInvalidRegistration
	}
	return nil
}

// Deregister under the given key.
func (co *Coordinator) Deregister(c context.Context, key string) error {
	co.mu.Lock()
	defer co.mu.Unlock()

	select {
	case <-co.done:
		// Nothing to unregister, coordinator is already
		// shutdown. "Deregistration" will be done by
		// Etcd deleting all keys associated with the
		// coordinator's lease.
		return nil
	default:
	}

	var version int64
	getRes, err := co.kv.Get(c, key)
	if err != nil {
		return err
	}
	if getRes != nil && len(getRes.Kvs) > 1 {
		return ErrInvalidRegistrationRecord
	}
	if getRes != nil && len(getRes.Kvs) == 1 {
		version = getRes.Kvs[0].Version
	} else {
		return ErrAlreadyDeregistered
	}
	rec := &regRec{}
	err = json.Unmarshal(getRes.Kvs[0].Value, rec)
	if err != nil {
		return err
	}
	if rec.Address != co.address {
		return ErrInvalidReceiverOwnership
	}

	txnRes, err := co.kv.Txn(c).
		If(etcdv3.Compare(etcdv3.Version(key), "=", version)).
		Then(etcdv3.OpDelete(key)).
		Commit()
	if err != nil {
		return err
	}
	if !txnRes.Succeeded {
		return ErrInvalidDeregistration
	}
	return nil
}
