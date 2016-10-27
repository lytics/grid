package discovery

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestStartStop(t *testing.T) {
	address := fmt.Sprintf("localhost:%v", 2000+rand.Intn(2000))

	co, err := New(address, []string{"localhost:2379"})
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
