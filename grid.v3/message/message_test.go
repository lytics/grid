package message

import (
	"context"
	"encoding/gob"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/lytics/grid/grid.v3/discovery"
)

type FooReqMsg struct {
	Cnt int
}

type FooResMsg struct {
	Cnt int
}

func init() {
	gob.Register(&FooReqMsg{})
	gob.Register(&FooResMsg{})
}

func TestFoo(t *testing.T) {
	runtime.GOMAXPROCS(8)

	cfg := etcdv3.Config{
		Endpoints: []string{"localhost:2379"},
	}
	client, err := etcdv3.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	address := fmt.Sprintf("localhost:%v", 2000+rand.Intn(2000))
	co, err := discovery.New(address, client)
	if err != nil {
		t.Fatal(err)
	}

	err = co.Start()
	if err != nil {
		t.Fatal(err)
	}
	localCtx, cancel := context.WithCancel(co.Context())

	mm, err := New(co)
	if err != nil {
		t.Fatal(err)
	}

	sub, err := mm.Subscribe("testing.r0", 100)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	for id := 0; id < 8; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			var cnt int
			var start time.Time
			for {
				select {
				case <-localCtx.Done():
					fmt.Printf("sender-%v: msg/sec: %.2f\n", id, float64(cnt)/time.Now().Sub(start).Seconds())
					return
				default:
					if _, err := mm.Request(1*time.Second, "testing.r0", &FooReqMsg{
						Cnt: cnt,
					}); err != nil {
						fmt.Printf("error: %v\n", err)
					}
					if cnt == 0 {
						start = time.Now()
					}
					cnt++
					if cnt%10000 == 0 {
						fmt.Printf("sender-%v: msg/sec: %.2f\n", id, float64(cnt)/time.Now().Sub(start).Seconds())
					}
				}
			}
		}(id)
	}

	go func() {
		cnt := 0
		for {
			select {
			case e := <-sub.Mailbox():
				e.Respond(&FooResMsg{Cnt: cnt})
			}
			cnt++
		}
	}()

	go func() {
		time.Sleep(20 * time.Second)
		cancel()
		wg.Wait()
		err := sub.Unsubscribe()
		if err != nil {
			fmt.Printf("unsub error: %v\n", err)
		}
		mm.Stop()
	}()

	// Will block until Stop is called.
	mm.Serve()

	// Stop the discovery coordinator.
	co.Stop()
}
