package main

import (
	"encoding/gob"
	"io"
	"log"

	"github.com/mdmarek/grid"
)

type MyMesg struct {
	Data int
}

type coder struct {
	*gob.Encoder
	*gob.Decoder
}

func (c *coder) New() interface{} {
	return &MyMesg{}
}

func NewMyMesgDecoder(r io.Reader) grid.Decoder {
	return &coder{nil, gob.NewDecoder(r)}
}

func NewMyMesgEncoder(w io.Writer) grid.Encoder {
	return &coder{gob.NewEncoder(w), nil}
}

func main() {

	gob.Register(MyMesg{})

	g, err := grid.New("test-grid")
	if err != nil {
		log.Fatalf("error: example: failed to create grid: %v", err)
	}

	g.AddDecoder("topic1", NewMyMesgDecoder)
	g.AddDecoder("topic2", NewMyMesgDecoder)
	g.AddDecoder("topic3", NewMyMesgDecoder)

	g.AddEncoder("topic1", NewMyMesgEncoder)
	g.AddEncoder("topic2", NewMyMesgEncoder)
	g.AddEncoder("topic3", NewMyMesgEncoder)

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

func add(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)
		for m := range in {
			switch mesg := m.Message().(type) {
			case MyMesg:
				out <- grid.NewWritable("topic2", "", MyMesg{Data: 1 + mesg.Data})
			default:
				log.Printf("error: example: unknown message: %T :: %v", mesg, mesg)
			}
		}
	}()

	return out
}

func mul(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)
		for m := range in {
			switch mesg := m.Message().(type) {
			case MyMesg:
				out <- grid.NewWritable("topic3", "", MyMesg{Data: 2 * mesg.Data})
			default:
				log.Printf("error: example: unknown message: %T :: %v", mesg, mesg)
			}
		}
	}()

	return out
}
