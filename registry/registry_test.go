package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lytics/grid/v3/testetcd"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

const (
	start     = true
	dontStart = false
)

var etcdEndpoints []string

func TestMain(m *testing.M) {
	embed := testetcd.NewEmbedded()
	defer embed.Etcd.Close()
	etcdEndpoints = []string{embed.Cfg.ACUrls[0].String()}
	r := m.Run()
	os.Exit(r)
}

func TestInitialLeaseID(t *testing.T) {
	client, r, _ := bootstrap(t, dontStart)
	defer client.Close()

	if r.leaseID != -1 {
		t.Fatal("lease id not initialized correctly")
	}
}

func TestStartStop(t *testing.T) {
	client, r, _ := bootstrap(t, start)
	defer client.Close()

	r.Stop()
	select {
	case <-r.done:
	default:
		t.Fatal("registry failed to stop")
	}
}

func TestStartStopWaitForLeaseToExpireBetween(t *testing.T) {
	client, r, addr := bootstrap(t, start)
	defer client.Close()

	// this should remove the lease which should clean up the registry lock on the address
	// which allows the next call to Start to begin without waiting.
	r.Stop()
	select {
	case <-r.done:
	default:
		t.Fatal("registry failed to stop")
	}

	st := time.Now()
	r, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	r.LeaseDuration = 10 * time.Second
	err = r.Start(addr)
	if err != nil {
		t.Fatal(err)
	}
	rt := time.Since(st)
	if rt > (time.Second) {
		t.Fatalf("runtime was too long, maybe because of waiting for leases to expire? ")
	}

	r.Stop()
	select {
	case <-r.done:
	default:
		t.Fatal("registry failed to stop")
	}
}

func TestWaitForLeaseThatNeverExpires(t *testing.T) {
	t.Parallel()

	start := false
	client, _, addr := bootstrap(t, start)
	defer client.Close()

	kv := etcdv3.NewKV(client)

	// synthetically create the lock
	address, err := formatAddress(addr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = kv.Put(context.Background(), registryLockKey(address), "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// cleanup for test
		_, err = kv.Delete(context.Background(), registryLockKey(address))
		if err != nil {
			t.Fatal(err)
		}
	}()

	r1, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	r1.LeaseDuration = 10 * time.Second

	st := time.Now()
	err = r1.Start(addr)
	if err == nil {
		t.Fatalf("expected an error but got none")
	}
	if err != ErrFailedAcquireAddressLock {
		t.Fatalf("expected an error `lease exists after timeout`, got %v", err)
	}

	// ensure that we waited 10 seconds...
	rt := time.Since(st)
	if rt < (10 * time.Second) {
		t.Fatalf("runtime was too short, it should have tried for at least 2*LeaseDuration? ")
	}
}

func TestWaitForLeaseThatDoesExpires(t *testing.T) {
	t.Parallel()

	start := false
	client, _, addr := bootstrap(t, start)
	defer client.Close()

	kv := etcdv3.NewKV(client)

	// synthetically create the lock
	address, err := formatAddress(addr)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	r1.LeaseDuration = 10 * time.Second

	_, err = kv.Put(context.Background(), registryLockKey(address), "")
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(5*time.Second, func() {
		// cleanup lock so that the registry can startup.
		_, err = kv.Delete(context.Background(), registryLockKey(address))
		if err != nil {
			t.Fatal(err)
		}
	})

	st := time.Now()
	err = r1.Start(addr)
	if err != nil {
		t.Fatalf("unexpected error: err: %v", err)
	}
	// ensure that we waited 10 seconds...
	rt := time.Since(st)
	if rt < (4 * time.Second) {
		t.Fatalf("runtime was too short, we didn't free the lock until 5 seconds")
	}
	if rt > (7 * time.Second) {
		t.Fatalf("runtime was too long, we freed the lock after 5 seconds")
	}

	r1.Stop()
	select {
	case <-r1.done:
	default:
		t.Fatal("registry failed to stop")
	}
}

func TestRegister(t *testing.T) {
	client, r, _ := bootstrap(t, start)
	defer client.Close()
	defer r.Stop()

	timeout, cancel := timeoutContext()
	err := r.Register(timeout, "test-registration")
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.Get(timeout, "test-registration")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	reg := &Registration{}
	err = json.Unmarshal(res.Kvs[0].Value, reg)
	if err != nil {
		t.Fatal(err)
	}
	if reg.Address != r.Address() {
		t.Fatal("wrong address")
	}
	if reg.Registry != r.Registry() {
		t.Fatal("wrong name")
	}
}

func TestDeregistration(t *testing.T) {
	client, r, _ := bootstrap(t, start)
	defer client.Close()
	defer r.Stop()

	timeout, cancel := timeoutContext()
	err := r.Register(timeout, "test-registration")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	res, err := client.Get(timeout, "test-registration")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	reg := &Registration{}
	err = json.Unmarshal(res.Kvs[0].Value, reg)
	if err != nil {
		t.Fatal(err)
	}
	if reg.Address != r.Address() {
		t.Fatal("wrong address")
	}
	if reg.Registry != r.Registry() {
		t.Fatal("wrong name")
	}

	timeout, cancel = timeoutContext()
	err = r.Deregister(timeout, "test-registration")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	res, err = client.Get(timeout, "test-registration")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 0 {
		t.Fatal("failed to deregister")
	}
}

func TestRegisterDeregisterWhileNotStarted(t *testing.T) {
	client, r, _ := bootstrap(t, dontStart)
	defer client.Close()
	defer r.Stop()

	timeout, cancel := timeoutContext()
	err := r.Register(timeout, "test-registration")
	cancel()
	if err != ErrNotStarted {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	err = r.Deregister(timeout, "test-registration")
	cancel()
	if err != ErrNotStarted {
		t.Fatal(err)
	}
}

func TestRegisterTwiceNotAllowed(t *testing.T) {
	client, r, _ := bootstrap(t, start)
	defer client.Close()
	defer r.Stop()

	for i := 0; i < 2; i++ {
		timeout, cancel := timeoutContext()
		err := r.Register(timeout, "test-registration-twice-b")
		cancel()
		if i > 0 && !errors.Is(err, ErrAlreadyRegistered) {
			t.Fatal("allowed to register twice")
		}
	}
}

func TestStop(t *testing.T) {
	client, r, _ := bootstrap(t, start)
	defer client.Close()

	timeout, cancel := timeoutContext()
	err := r.Register(timeout, "test-registration")
	if err != nil {
		t.Fatal(err)
	}

	r.Stop()
	res, err := client.Get(timeout, "test-registration")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 0 {
		t.Fatal("stopping heartbeat did not delete keys")
	}
}

func TestFindRegistration(t *testing.T) {
	client, r, _ := bootstrap(t, start)
	defer client.Close()
	defer r.Stop()

	timeout, cancel := timeoutContext()
	err := r.Register(timeout, "test-registration-a")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	err = r.Register(timeout, "test-registration-aa")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	reg, err := r.FindRegistration(timeout, "test-registration-a")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if reg.Key != "test-registration-a" {
		t.Fatal("got wrong key")
	}
}

func TestFindRegistrations(t *testing.T) {
	client, r, _ := bootstrap(t, start)
	defer client.Close()
	defer r.Stop()

	timeout, cancel := timeoutContext()
	err := r.Register(timeout, "test-registration-a")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	err = r.Register(timeout, "test-registration-aa")
	cancel()
	if err != nil {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	regs, err := r.FindRegistrations(timeout, "test-registration-a")
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if len(regs) != 2 {
		t.Fatal("failed to find number of expected registrations")
	}

	expected := map[string]bool{
		"test-registration-a":  true,
		"test-registration-aa": true,
	}
	for _, reg := range regs {
		delete(expected, reg.Key)
	}
	if len(expected) != 0 {
		t.Fatal("failed to find all expected registrations")
	}
}

func TestKeepAlive(t *testing.T) {
	client, r, addr := bootstrap(t, dontStart)
	defer client.Close()

	// Change the minimum for sake of testing quickly.
	minLeaseDuration = 1 * time.Second

	// Use the minimum.
	r.LeaseDuration = 1 * time.Second
	err := r.Start(addr)
	if err != nil {
		t.Fatal(err)
	}

	// Check that the number of heartbeats matching some reasonable
	// expected minimum. Keep in mind that each lease duration
	// should produce "hearbratsPerLeaseDuration" heartbeats.
	time.Sleep(5 * time.Second)
	r.Stop()
	if r.keepAliveStats.success < 1 {
		t.Fatal("expected at least one successful heartbeat")
	}
}

func TestWatch(t *testing.T) {
	client, r, addr := bootstrap(t, dontStart)
	defer client.Close()

	// Change the minimum for sake of testing quickly.
	minLeaseDuration = 1 * time.Second

	// Use the minimum.
	r.LeaseDuration = 1 * time.Second
	err := r.Start(addr)
	if err != nil {
		t.Fatal(err)
	}

	initial := map[string]bool{
		"peer-1": true,
		"peer-2": true,
		"peer-3": true,
	}

	for peer := range initial {
		timeout, cancel := timeoutContext()
		err := r.Register(timeout, peer)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}

	watchStarted := make(chan bool)
	watchErrors := make(chan error)
	watchAdds := make(map[string]bool)
	watchDels := make(map[string]bool)
	ctx, cancelWatch := context.WithCancel(context.Background())
	go func() {
		current, events, err := r.Watch(ctx, "peer")
		if err != nil {
			watchErrors <- err
			return
		}
		for _, peer := range current {
			if !initial[peer.Key] {
				watchErrors <- fmt.Errorf("missing initial peer: " + peer.Key)
				return
			}
		}
		close(watchStarted)
		for e := range events {
			switch e.Type {
			case Delete:
				watchDels[e.Key] = true
			case Create:
				watchAdds[e.Key] = true
			}
		}
		close(watchErrors)
	}()

	<-watchStarted

	additions := map[string]bool{
		"peer-4": true,
		"peer-5": true,
	}

	for peer := range additions {
		timeout, cancel := timeoutContext()
		err := r.Register(timeout, peer)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}

	for peer := range initial {
		timeout, cancel := timeoutContext()
		err := r.Deregister(timeout, peer)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}

	cancelWatch()

	err = <-watchErrors
	if err != nil {
		t.Fatal(err)
	}

	for k := range initial {
		delete(watchDels, k)
	}
	if len(watchDels) != 0 {
		t.Fatalf("failed to watch expected deletions: %v", initial)
	}

	for k := range additions {
		delete(watchAdds, k)
	}
	if len(watchAdds) != 0 {
		t.Fatalf("failed to watch expected additions: %v", additions)
	}

	time.Sleep(3 * time.Second)
	r.Stop()
}

func TestWatchEventString(t *testing.T) {
	we := &WatchEvent{
		Key:  "foo",
		Type: Create,
		Reg: &Registration{
			Key:      "foo",
			Address:  "localhost:7777",
			Registry: "goo",
		},
	}

	// Create
	if !strings.Contains(we.String(), "create") {
		t.Fatal("watch event string is invalid")
	}
	// Modify
	we.Type = Modify
	if !strings.Contains(we.String(), "modify") {
		t.Fatal("watch event string is invalid")
	}
	// Delete
	we.Type = Delete
	if !strings.Contains(we.String(), "delete") {
		t.Fatal("watch event string is invalid")
	}
	// Error
	we.Type = Error
	we.Error = errors.New("watch event testing error")
	if !strings.Contains(we.String(), "error") {
		t.Fatal("watch event string is invalid")
	}
}

func bootstrap(t *testing.T, shouldStart bool) (*etcdv3.Client, *Registry, *net.TCPAddr) {
	client := testetcd.StartAndConnect(t, etcdEndpoints)

	addr := &net.TCPAddr{
		IP:   []byte("localhost"),
		Port: 2000 + rand.Intn(2000),
	}

	r, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	r.LeaseDuration = 10 * time.Second

	if shouldStart {
		err = r.Start(addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	return client, r, addr
}

func timeoutContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}
