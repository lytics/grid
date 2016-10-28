package discovery

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"encoding/json"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

const (
	start     = true
	dontStart = false
)

func TestInitialLeaseID(t *testing.T) {
	client, co := bootstrap(t, dontStart)
	defer client.Close()

	if co.leaseID != -1 {
		t.Fatal("lease id not initialized correctly")
	}
}

func TestStartStop(t *testing.T) {
	client, co := bootstrap(t, start)
	defer client.Close()

	finished := make(chan bool, 1)
	go func() {
		defer close(finished)
		select {
		case <-time.After(10 * time.Second):
			finished <- false
			return
		case <-co.Context().Done():
			finished <- true
			return
		}
	}()

	co.StopHeartbeat()
	isFinished := <-finished
	if !isFinished {
		t.Fatal("coordinator failed to finish")
	}
}

func TestRegister(t *testing.T) {
	client, co := bootstrap(t, start)
	defer client.Close()
	defer co.StopHeartbeat()

	timeout, cancel := timeoutContext()
	err := co.Register(timeout, "test-registration")
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
	if reg.Address != co.address {
		t.Fatal("wrong address")
	}
}

func TestDeregistration(t *testing.T) {
	client, co := bootstrap(t, start)
	defer client.Close()
	defer co.StopHeartbeat()

	timeout, cancel := timeoutContext()
	err := co.Register(timeout, "test-registration")
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
	if reg.Address != co.address {
		t.Fatal("wrong address")
	}

	timeout, cancel = timeoutContext()
	err = co.Deregister(timeout, "test-registration")
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
	client, co := bootstrap(t, dontStart)
	defer client.Close()
	defer co.StopHeartbeat()

	timeout, cancel := timeoutContext()
	err := co.Register(timeout, "test-registration")
	cancel()
	if err != ErrNotStarted {
		t.Fatal(err)
	}

	timeout, cancel = timeoutContext()
	err = co.Deregister(timeout, "test-registration")
	cancel()
	if err != ErrNotStarted {
		t.Fatal(err)
	}
}

func TestRegisterTwiceAllowed(t *testing.T) {
	client, co := bootstrap(t, start)
	defer client.Close()
	defer co.StopHeartbeat()

	for i := 0; i < 2; i++ {
		timeout, cancel := timeoutContext()
		err := co.Register(timeout, "test-registration-twice", OpAllowReentrantRegistrations)
		cancel()
		if i > 0 && err == ErrAlreadyRegistered {
			t.Fatal("not allowed to register twice")
		}
	}
}

func TestRegisterTwiceNotAllowed(t *testing.T) {
	client, co := bootstrap(t, start)
	defer client.Close()
	defer co.StopHeartbeat()

	for i := 0; i < 2; i++ {
		timeout, cancel := timeoutContext()
		err := co.Register(timeout, "test-registration-twice-b")
		cancel()
		if i > 0 && err != ErrAlreadyRegistered {
			t.Fatal("allowed to register twice")
		}
	}
}

func TestStopHeartbeat(t *testing.T) {
	client, co := bootstrap(t, start)
	defer client.Close()

	timeout, cancel := timeoutContext()
	err := co.Register(timeout, "test-registration")
	if err != nil {
		t.Fatal(err)
	}

	co.StopHeartbeat()
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

}

func bootstrap(t *testing.T, shouldStart bool) (*etcdv3.Client, *Coordinator) {
	cfg := etcdv3.Config{
		Endpoints: []string{"localhost:2379"},
	}
	client, err := etcdv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	address := fmt.Sprintf("localhost:%v", 2000+rand.Intn(2000))
	co, err := New(address, client)
	if err != nil {
		t.Fatal(err)
	}
	co.LeaseDuration = 10 * time.Second

	if shouldStart {
		err = co.StartHeartbeat()
		if err != nil {
			t.Fatal(err)
		}
	}

	return client, co
}

func timeoutContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 2*time.Second)
}
