package main

import (
	"encoding/json"
	"io"
	"log"

	"github.com/lytics/grid"
)

const Key = ""

type NumMesg struct {
	Data int
}

func NewNumMesg(i int) *NumMesg {
	return &NumMesg{i}
}

type numcoder struct {
	*json.Encoder
	*json.Decoder
}

func (c *numcoder) New() interface{} {
	return &NumMesg{}
}

func NewNumMesgDecoder(r io.Reader) grid.Decoder {
	return &numcoder{nil, json.NewDecoder(r)}
}

func NewNumMesgEncoder(w io.Writer) grid.Encoder {
	return &numcoder{json.NewEncoder(w), nil}
}

func main() {

	g, err := grid.New("test-grid")
	if err != nil {
		log.Fatalf("error: example: failed to create grid: %v", err)
	}

	g.AddDecoder(NewNumMesgDecoder, "topic1", "topic2", "topic3")
	g.AddEncoder(NewNumMesgEncoder, "topic1", "topic2", "topic3")

	err = g.Add("add", 1, add)
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}

	g.Read("add", "topic1")
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}
	g.Write("add", "topic2")
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}

	g.Add("mul", 1, mul)
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}
	g.Read("mul", "topic2")
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}
	g.Write("mul", "topic3")
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
		for e := range in {
			switch mesg := e.Message().(type) {
			case *NumMesg:
				out <- grid.NewWritable(Key, NewNumMesg(1+mesg.Data))
			default:
				log.Printf("example: unknown message: %T :: %v", mesg, mesg)
			}
		}
	}()

	return out
}

func mul(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)
		for e := range in {
			switch mesg := e.Message().(type) {
			case *NumMesg:
				out <- grid.NewWritable(Key, NewNumMesg(2*mesg.Data))
			default:
				log.Printf("example: unknown message: %T :: %v", mesg, mesg)
			}
		}
	}()

	return out
}
