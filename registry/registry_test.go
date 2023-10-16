package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
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

func TestInitialLeaseID(t *testing.T) {
	t.Parallel()
	_, r, _ := bootstrap(t, dontStart)

	if r.leaseID != -1 {
		t.Fatal("lease id not initialized correctly")
	}
}

func TestStartStop(t *testing.T) {
	t.Parallel()
	_, r, _ := bootstrap(t, start)

	err := r.Stop()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.done:
	default:
		t.Fatal("registry failed to stop")
	}
}

func TestStartStopWaitForLeaseToExpireBetween(t *testing.T) {
	t.Parallel()
	client, r, addr := bootstrap(t, start)

	// this should remove the lease which should clean up the registry lock on the address
	// which allows the next call to Start to begin without waiting.
	err := r.Stop()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.done:
	default:
		t.Fatal("registry failed to stop")
	}

	st := time.Now()
	r, err = New(client)
	if err != nil {
		t.Fatal(err)
	}
	r.LeaseDuration = 10 * time.Second
	err = r.Start(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	rt := time.Since(st)
	if rt > (time.Second) {
		t.Fatalf("runtime was too long, maybe because of waiting for leases to expire? ")
	}

	err = r.Stop()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.done:
	default:
		t.Fatal("registry failed to stop")
	}
}

func TestWaitForLeaseThatNeverExpires(t *testing.T) {
	t.Parallel()

	client, _, addr := bootstrap(t, dontStart)

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
	t.Cleanup(func() {
		// cleanup for test
		_, err = kv.Delete(context.Background(), registryLockKey(address))
		if err != nil {
			t.Fatal(err)
		}
	})

	r1, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	r1.LeaseDuration = 10 * time.Second

	st := time.Now()
	err = r1.Start(context.Background(), addr)
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

	if _, err := kv.Put(context.Background(), registryLockKey(address), ""); err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(5*time.Second, func() {
		// cleanup lock so that the registry can startup.
		if _, err := kv.Delete(context.Background(), registryLockKey(address)); err != nil {
			t.Fatal(err)
		}
	})

	st := time.Now()
	if err := r1.Start(context.Background(), addr); err != nil {
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

	err = r1.Stop()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r1.done:
	default:
		t.Fatal("registry failed to stop")
	}
}

func TestRegister(t *testing.T) {
	t.Parallel()
	client, r, _ := bootstrap(t, start)

	ctx, cancel := timeoutContext()
	defer cancel()
	key := "test-registration" + fmt.Sprint(rand.Intn(1000))
	err := r.Register(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.Get(ctx, key)
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
	t.Parallel()
	client, r, _ := bootstrap(t, start)

	ctx, cancel := timeoutContext()
	defer cancel()
	key := "test-registration" + fmt.Sprint(rand.Intn(1000))
	err := r.Register(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = timeoutContext()
	defer cancel()
	res, err := client.Get(ctx, key)
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

	ctx, cancel = timeoutContext()
	defer cancel()
	err = r.Deregister(ctx, "test-registration")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = timeoutContext()
	defer cancel()
	res, err = client.Get(ctx, "test-registration")
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 0 {
		t.Fatal("failed to deregister")
	}
}

func TestRegisterDeregisterWhileNotStarted(t *testing.T) {
	t.Parallel()
	_, r, _ := bootstrap(t, dontStart)

	ctx, cancel := timeoutContext()
	defer cancel()
	err := r.Register(ctx, "test-registration")
	if err != ErrNotStarted {
		t.Fatal(err)
	}

	ctx, cancel = timeoutContext()
	defer cancel()
	err = r.Deregister(ctx, "test-registration")
	if err != ErrNotStarted {
		t.Fatal(err)
	}
}

func TestRegisterTwiceNotAllowed(t *testing.T) {
	t.Parallel()
	_, r, _ := bootstrap(t, start)

	for i := 0; i < 2; i++ {
		ctx, cancel := timeoutContext()
		defer cancel()
		err := r.Register(ctx, "test-registration-twice-b")
		if i > 0 && !errors.Is(err, ErrAlreadyRegistered) {
			t.Fatal("allowed to register twice")
		}
	}
}

func TestStop(t *testing.T) {
	t.Parallel()
	client, r, _ := bootstrap(t, start)

	ctx, cancel := timeoutContext()
	defer cancel()
	err := r.Register(ctx, "test-registration")
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
	res, err := client.Get(ctx, "test-registration")
	if err != nil {
		t.Fatal(err)
	}
	if res.Count != 0 {
		t.Fatal("stopping heartbeat did not delete keys")
	}
}

func TestFindRegistration(t *testing.T) {
	t.Parallel()
	_, r, _ := bootstrap(t, start)

	ctx, cancel := timeoutContext()
	defer cancel()
	err := r.Register(ctx, "test-registration-a")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = timeoutContext()
	defer cancel()
	err = r.Register(ctx, "test-registration-aa")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = timeoutContext()
	defer cancel()
	reg, err := r.FindRegistration(ctx, "test-registration-a")
	if err != nil {
		t.Fatal(err)
	}
	if reg.Key != "test-registration-a" {
		t.Fatal("got wrong key")
	}
}

func TestFindRegistrations(t *testing.T) {
	t.Parallel()
	_, r, _ := bootstrap(t, start)

	ctx, cancel := timeoutContext()
	defer cancel()
	key1 := "test-registration-a" + fmt.Sprint(rand.Intn(1000))
	err := r.Register(ctx, key1)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = timeoutContext()
	defer cancel()
	key2 := "test-registration-a" + fmt.Sprint(rand.Intn(1000))
	err = r.Register(ctx, key2)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = timeoutContext()
	defer cancel()
	regs, err := r.FindRegistrations(ctx, "test-registration-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(regs) != 2 {
		t.Fatal("failed to find number of expected registrations")
	}

	expected := map[string]bool{
		key1: true,
		key2: true,
	}
	for _, reg := range regs {
		delete(expected, reg.Key)
	}
	if len(expected) != 0 {
		t.Fatal("failed to find all expected registrations")
	}
}

func TestKeepAlive(t *testing.T) {
	t.Parallel()
	_, r, addr := bootstrap(t, dontStart)

	// Change the minimum for sake of testing quickly.
	minLeaseDuration = 1 * time.Second

	// Use the minimum.
	r.LeaseDuration = 1 * time.Second
	err := r.Start(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}

	// Check that the number of heartbeats matching some reasonable
	// expected minimum. Keep in mind that each lease duration
	// should produce "hearbratsPerLeaseDuration" heartbeats.
	time.Sleep(5 * time.Second)
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
	if r.keepAliveStats.success < 1 {
		t.Fatal("expected at least one successful heartbeat")
	}
}

func TestWatch(t *testing.T) {
	t.Parallel()
	_, r, addr := bootstrap(t, dontStart)

	// Change the minimum for sake of testing quickly.
	minLeaseDuration = 1 * time.Second

	// Use the minimum.
	r.LeaseDuration = 1 * time.Second
	err := r.Start(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}

	initial := map[string]bool{
		"peer-1": true,
		"peer-2": true,
		"peer-3": true,
	}

	for peer := range initial {
		ctx, cancel := timeoutContext()
		err := r.Register(ctx, peer)
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
		ctx, cancel := timeoutContext()
		err := r.Register(ctx, peer)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
	}

	for peer := range initial {
		ctx, cancel := timeoutContext()
		err := r.Deregister(ctx, peer)
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
	if err := r.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchEventString(t *testing.T) {
	t.Parallel()
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

func bootstrap(t testing.TB, shouldStart bool) (*etcdv3.Client, *Registry, *net.TCPAddr) {
	t.Helper()
	embed := testetcd.NewEmbedded(t)
	client := testetcd.StartAndConnect(t, embed.Endpoints())

	addr := &net.TCPAddr{
		IP:   []byte("localhost"),
		Port: 2000 + rand.Intn(2000),
	}

	r, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	r.LeaseDuration = 10 * time.Second
	t.Cleanup(func() {
		if err := r.Stop(); err != nil {
			t.Fatal(err)
		}
	})

	if shouldStart {
		err = r.Start(context.Background(), addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	return client, r, addr
}

func timeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}
