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

func main() {
	flag.Parse()

	kconf := grid.DefaultKafkaConfig()
	kconf.Brokers = strings.Split(*kafka, ",")

	g, err := grid.NewWithKafkaConfig(GridName, *peercnt, kconf)
	if err != nil {
		log.Fatalf("error: example: failed to create grid: %v", err)
	}

	g.AddDecoder(NewNumMesgDecoder, "topic1", "topic2", "topic3")
	g.AddEncoder(NewNumMesgEncoder, "topic1", "topic2", "topic3")

	g.Add("add", 2, add, "topic1")
	g.Add("mul", 2, mul, "topic2")
	g.Add("readline", 1, readline, "topic3")

	g.Start()
	g.Wait()
}

func add(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)
		var ready bool

		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewUseOffset(mesg.Topic, mesg.Part, mesg.Max)
			case grid.Ready:
				ready = true
			default:
			}
			if ready {
				break
			}
		}

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

func mul(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)
		var ready bool

		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewUseOffset(mesg.Topic, mesg.Part, mesg.Max)
			case grid.Ready:
				ready = true
			default:
			}
			if ready {
				break
			}
		}

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

func readline(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)
		var ready bool

		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewUseOffset(mesg.Topic, mesg.Part, mesg.Max)
			case grid.Ready:
				ready = true
			default:
			}
			if ready {
				break
			}
		}

		var i int
		fmt.Printf("\nenter a number: ")
		if _, err := fmt.Scanf("%d", &i); err != nil {
			fmt.Printf("\nerror: that's not a number")
		} else {
			out <- grid.NewWritable("topic1", strconv.Itoa(i), NewNumMesg(i))
		}

		// Read from topic3, think of it as the result topic.
		for event := range in {
			switch mesg := event.Message().(type) {
			case *NumMesg:
				if "topic3" == event.Topic() {
					fmt.Printf("\nresult: %v", mesg.Data)
				}
			default:
			}

			for {
				fmt.Printf("\nenter a number: ")
				if _, err := fmt.Scanf("%d", &i); err != nil {
					fmt.Printf("\nerror: that's not a number")
				} else {
					out <- grid.NewWritable("topic1", strconv.Itoa(i), NewNumMesg(i))
					break
				}
			}
		}
	}()

	return out
}
