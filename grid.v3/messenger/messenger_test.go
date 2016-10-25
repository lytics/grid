package messenger

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"
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
	defer cancel()

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	address := fmt.Sprintf("%v:7777", hostname)
	nx, err := New(address, []string{"http://localhost:2379"})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nx.Subscribe(ctx, "testing", "r0", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	go func() {
		cnt := 0
		for {
			time.Sleep(1 * time.Second)

			msg := &FooReqMsg{Cnt: cnt}
			fmt.Printf("req: %+v\n", msg)

			timeout, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			e, err := nx.Request(timeout, "testing", "r0", msg)
			if err != nil {
				fmt.Printf("error: %v\n", err)
			} else {
				fmt.Printf("res: %+v\n", e.Msg)
			}
			cnt++
		}
	}()

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

	err = nx.Serve()
	if err != nil {
		t.Fatal(err)
	}
}
