package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/lytics/grid"
)

const (
	Key          = ""
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
var kafka = flag.String("kafka", "localhost:10092", "listof kafka brokers, for example: localhost:10092,localhost:10093")

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
	g.Add("add", 2, NewAdder(), "topic1")
	g.Add("mul", 2, NewMultiplier(), "topic2")
	g.Add("readline", 1, NewReader(), "topic3")

	// Start and wait for exit.
	g.Start()
	g.Wait()
}

type add struct{}

func NewAdder() grid.Actor { return &add{} }

func (*add) Act(in <-chan grid.Event) <-chan grid.Event {

	out := make(chan grid.Event)

	go func() {
		defer close(out)

		// Before doing any real work, each function must read its MinMaxOffset
		// messages and respond to each with a UseOffset message. The offset
		// chosen can of course be retrieved from anywhere, but it must be
		// between the min and max value.
		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewUseOffset(mesg.Topic, mesg.Part, mesg.Max)
			case grid.Ready:
				goto Ready
			default:
			}
		}

	Ready:
		// After requesting the offsets, the in channel will contain messages
		// from the actual input topics, starting at the requested offsets.
		for event := range in {
			switch mesg := event.Message().(type) {
			case *NumMesg:
				outmsg := 1 + mesg.Data
				out <- grid.NewWritable("topic2", Key, NewNumMesg(outmsg))
				log.Printf("add(): %d -> %d\n", mesg.Data, outmsg)
			default:
			}
		}
	}()

	return out
}

type mul struct{}

func NewMultiplier() grid.Actor { return &mul{} }

func (*mul) Act(in <-chan grid.Event) <-chan grid.Event {

	out := make(chan grid.Event)

	go func() {
		defer close(out)

		// Before doing any real work, each function must read its MinMaxOffset
		// messages and respond to each with a UseOffset message. The offset
		// chosen can of course be retrieved from anywhere, but it must be
		// between the min and max value.
		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewUseOffset(mesg.Topic, mesg.Part, mesg.Max)
			case grid.Ready:
				goto Ready
			default:
			}
		}

	Ready:
		// After requesting the offsets, the in channel will contain messages
		// from the actual input topics, starting at the requested offsets.
		for event := range in {
			switch mesg := event.Message().(type) {
			case *NumMesg:
				outmsg := 2 * mesg.Data
				out <- grid.NewWritable("topic3", Key, NewNumMesg(outmsg))
				log.Printf("mul(): %d -> %d\n", mesg.Data, outmsg)
			default:
			}
		}
	}()

	return out
}

type reader struct{}

func NewReader() grid.Actor { return &reader{} }

func (*reader) Act(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)

		// Before doing any real work, each function must read its MinMaxOffset
		// messages and respond to each with a UseOffset message. The offset
		// chosen can of course be retrieved from anywhere, but it must be
		// between the min and max value.
		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewUseOffset(mesg.Topic, mesg.Part, mesg.Max)
			case grid.Ready:
				goto Ready
			default:
			}
		}

	Ready:
		// Start things off with an initial message.
		i := readnumber()
		out <- grid.NewWritable("topic1", strconv.Itoa(i), NewNumMesg(i))

		// After requesting the offsets, the in channel will contain messages
		// from the actual input topics, starting at the requested offsets.
		for event := range in {
			switch mesg := event.Message().(type) {
			case *NumMesg:
				if "topic3" == event.Topic() {
					fmt.Printf("\nresult: %v", mesg.Data)
				}
			default:
			}
			i = readnumber()
			out <- grid.NewWritable("topic1", strconv.Itoa(i), NewNumMesg(i))
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
