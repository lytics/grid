package main

import (
	"log"
	"strconv"

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

	err = g.Add(1, add, "topic1")
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}

	err = g.Add(1, mul, "topic2")
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}

	g.Start()
	g.Wait()
}

func add(in <-chan grid.Mesg) <-chan grid.Mesg {
	out := make(chan grid.Mesg)

	go func(in <-chan grid.Mesg, out chan<- grid.Mesg) {
		defer close(out)

		for m := range in {
			numstr := string(m.Value())
			if num, err := strconv.ParseInt(string(numstr), 10, 64); err != nil {
				log.Printf("error: example: add'er: topic: %v message not a number: %v", m.Topic(), numstr)
			} else {
				newnumstr := strconv.FormatInt(num+1, 10)
				out <- &mesg{grid.NewHeader("topic2"), newnumstr, newnumstr}
			}
		}
	}(in, out)

	return out
}

func mul(in <-chan grid.Mesg) <-chan grid.Mesg {
	out := make(chan grid.Mesg)

	go func(in <-chan grid.Mesg, out chan<- grid.Mesg) {
		defer close(out)

		for m := range in {
			numstr := string(m.Value())
			if num, err := strconv.ParseInt(string(numstr), 10, 64); err != nil {
				log.Printf("error: example: mul'er: topic: %v message not a number: %v", m.Topic(), numstr)
			} else {
				newnumstr := strconv.FormatInt(2*num, 10)
				out <- &mesg{grid.NewHeader("topic3"), newnumstr, newnumstr}
			}
		}
	}(in, out)

	return out
}
