package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/lytics/grid"
)

const (
	GridName     = "test-grid"
	ConsumerName = "tesg-grid-console-consumer"
)

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

var peercnt = flag.Int("peers", 1, "the expected number of peers that will take part in the grid")
var kafka = flag.String("kafka", "localhost:9092", "list of kafka brokers, for example: localhost:9092,localhost:9092")

// Runs the example; it can be started in a few different ways:
//
// Mixes log and interactive messages:
//    $ ./example
//
// Sends log to a file, leaves interactive messages on stdout:
//    $ ./example 2>log
//
// Starts the example expecting two peers, so make sure to start both:
//    $ ./example -peers 2
//
func main() {
	flag.Parse()

	kconf := grid.DefaultKafkaConfig()
	kconf.Brokers = strings.Split(*kafka, ",")

	g, err := grid.NewWithKafkaConfig(GridName, *peercnt, kconf)
	if err != nil {
		log.Fatalf("error: example: failed to create grid: %v", err)
	}

	// Set encoders and decoders.
	g.AddDecoder(NewNumMesgDecoder, "topic1", "topic2", "topic3")
	g.AddEncoder(NewNumMesgEncoder, "topic1", "topic2", "topic3")

	// Set processing layers.
	err = g.Add("add", 1, NewAdder(), "topic1")
	if err != nil {
		log.Fatalf("failed to start grid: %v", err)
	}
	err = g.Add("mul", 1, NewMultiplier(), "topic2")
	if err != nil {
		log.Fatalf("failed to start grid: %v", err)
	}
	err = g.Add("readline", 1, NewReader(), "topic3")
	if err != nil {
		log.Fatalf("failed to start grid: %v", err)
	}

	// Start and wait for exit.
	g.Start()
	g.Wait()
}

type add struct{}

func NewAdder() grid.NewActor {
	return func(name string, id int) grid.Actor { return &add{} }
}

func (*add) Act(in <-chan grid.Event, state <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)
	go func() {
		defer close(out)
		log.Printf("startring: '%v' actor", "add")
		for {
			select {
			case event := <-state:
				// Before doing any real work, each function must read its MinMaxOffset
				// messages and respond to each with a UseOffset message. The offset
				// chosen can of course be retrieved from anywhere, but it must be
				// between the min and max value.
				switch mesg := event.Message().(type) {
				case *grid.MinMaxOffset:
					mesg.UseMax()
				}
			case event := <-in:
				// After requesting the offsets, the in channel will contain messages
				// from the actual input topics, starting at the requested offsets.
				switch mesg := event.Message().(type) {
				case *NumMesg:
					outmsg := 1 + mesg.Data
					out <- grid.NewWritable("topic2", nil, NewNumMesg(outmsg))
					log.Printf("add(): %d -> %d\n", mesg.Data, outmsg)
				}
			}
		}
	}()
	return out
}

type mul struct{}

func NewMultiplier() grid.NewActor {
	return func(name string, id int) grid.Actor { return &mul{} }
}

func (*mul) Act(in <-chan grid.Event, state <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)
	go func() {
		defer close(out)
		log.Printf("startring: '%v' actor", "mul")
		for {
			select {
			case event := <-state:
				// Before doing any real work, each function must read its MinMaxOffset
				// messages and respond to each with a UseOffset message. The offset
				// chosen can of course be retrieved from anywhere, but it must be
				// between the min and max value.
				switch mesg := event.Message().(type) {
				case *grid.MinMaxOffset:
					mesg.UseMax()
				}
			case event := <-in:
				switch mesg := event.Message().(type) {
				case *NumMesg:
					outmsg := 2 * mesg.Data
					out <- grid.NewWritable("topic3", nil, NewNumMesg(outmsg))
					log.Printf("mul(): %d -> %d\n", mesg.Data, outmsg)
				}
			}
		}
	}()
	return out
}

type reader struct{}

func NewReader() grid.NewActor {
	return func(name string, id int) grid.Actor { return &reader{} }
}

func (*reader) Act(in <-chan grid.Event, state <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)
	go func() {
		defer close(out)
		log.Printf("startring: '%v' actor", "reader")
		// Start things off with an initial message.
		go func() {
			for {
				time.Sleep(5 * time.Second)
				i := readnumber()
				out <- grid.NewWritable("topic1", []byte(strconv.Itoa(i)), NewNumMesg(i))
			}
		}()
		for {
			select {
			case event := <-state:
				// Before doing any real work, each function must read its MinMaxOffset
				// messages and respond to each with a UseOffset message. The offset
				// chosen can of course be retrieved from anywhere, but it must be
				// between the min and max value.
				switch mesg := event.Message().(type) {
				case *grid.MinMaxOffset:
					mesg.UseMax()
				}
			case event := <-in:
				switch mesg := event.Message().(type) {
				case *NumMesg:
					if "topic3" == event.Topic() {
						fmt.Printf("\nresult: %v", mesg.Data)
					}
				}
			}
		}
	}()
	return out
}

func readnumber() int {
	var i int
	for {
		fmt.Printf("\nenter a number: ")
		if _, err := fmt.Scanf("%d", &i); err != nil {
			fmt.Printf("\nerror: that's not a number")
		} else {
			return i
		}
	}
}
