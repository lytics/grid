package messenger

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
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
	ctx, cancel := context.WithCancel(context.Background())

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	address := fmt.Sprintf("%v:7777", hostname)
	nx, err := New(address, []string{"http://localhost:2379"})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nx.Subscribe(ctx, "testing", "r0", 100)
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
				case <-ctx.Done():
					return
				default:
					msg := &FooReqMsg{Cnt: cnt}

					timeout, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					_, err := nx.Request(timeout, "testing", "r0", msg)
					cancel()

					if err != nil {
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
		err := sub.Unsubscribe(context.Background())
		if err != nil {
			fmt.Printf("unsub error: %v\n", err)
		}
		nx.Stop()
	}()

	// Will block until Stop is called.
	nx.Start()
}
