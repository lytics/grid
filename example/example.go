package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/mdmarek/grid"
)

type mesg struct {
	grid.Header
	key   string
	value string
}

func (m *mesg) Key() []byte {
	return []byte(m.key)
}

func (m *mesg) Value() []byte {
	return []byte(m.value)
}

func main() {
	g, err := grid.New("test-grid")
	if err != nil {
		log.Fatalf("error: example: failed to create grid: %v", err)
	}

	// Register signal handler.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	go func() {
		s := <-sig
		log.Printf("example: stopping: got signal: %v", s)
		g.Stop()
	}()

	// Start creating a grid of functions.
	err = g.Of(1, f, "foo")
	if err != nil {
		log.Printf("example: grid error: %v", err)
	}

	g.Start()
	g.Wait()
}

func f(in <-chan grid.Mesg) <-chan grid.Mesg {
	out := make(chan grid.Mesg)
	go func(in <-chan grid.Mesg, out chan<- grid.Mesg) {
		defer close(out)
		for m := range in {
			numstr := string(m.Value())
			if num, err := strconv.ParseInt(string(numstr), 10, 64); err != nil {
				log.Printf("error: example: topic: %v message not a number: %v", m.Topic(), numstr)
			} else {
				newnumstr := strconv.FormatInt(num+1, 10)
				out <- &mesg{grid.NewHeader("out"), newnumstr, newnumstr}
			}
		}
	}(in, out)
	return out
}
