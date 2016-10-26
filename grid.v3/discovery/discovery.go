package discovery

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
)

const keepAlivesPerLeaseDuration = 6

var (
	ErrUnknownAddress            = fmt.Errorf("unknown address")
	ErrInvalidRegistration       = fmt.Errorf("invalid registration")
	ErrAlreadyDeregistered       = fmt.Errorf("already deregistered")
	ErrInvalidDeregistration     = fmt.Errorf("invalid deregistration")
	ErrReceiverAlreadyExists     = fmt.Errorf("receiver already exists")
	ErrInvalidReceiverOwnership  = fmt.Errorf("invalid receiver ownership")
	ErrInvalidRegistrationRecord = fmt.Errorf("invalid registration record set")
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
	Timeout       time.Duration
	LeaseDuration time.Duration
}

func New(address string, servers []string) (*Coordinator, error) {
	cfg := etcdv3.Config{
		Endpoints: servers,
	}
	client, err := etcdv3.New(cfg)
	if err != nil {
		return nil, err
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
func (co *Coordinator) StartHeartbeat() (context.Context, error) {
	co.mu.Lock()
	defer co.mu.Unlock()

	timeout, cancel := context.WithTimeout(context.Background(), co.Timeout)
	defer cancel()

	res, err := co.lease.Grant(timeout, int64(co.LeaseDuration.Seconds()))
	if err != nil {
		return nil, err
	}
	co.leaseID = res.ID

	c, finalize := context.WithCancel(context.Background())
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
					fmt.Printf("keep alive error: %v\n", err)
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

	return c, nil
}

// StopHeartbeat.
func (co *Coordinator) StopHeartbeat() {
	close(co.done)
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

// RegisterReceiver under the given key. Once registered, senders can find
// the host address of the receiver with FindAddress.
func (co *Coordinator) RegisterReceiver(c context.Context, key string) error {
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
			return ErrReceiverAlreadyExists
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

// DeregisterReceiver under the given key.
func (co *Coordinator) DeregisterReceiver(c context.Context, key string) error {
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
