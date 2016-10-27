package discovery

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
)

func TestStartStop(t *testing.T) {
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

	err = co.StartHeartbeat()
	if err != nil {
		t.Fatal(err)
	}

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
