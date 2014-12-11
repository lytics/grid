package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/lytics/grid"
)

const (
	Key          = ""
	GridName     = "test-grid"
	ConsumerName = "tesg-grid-console-consumer"
)

type NumMesg struct {
	Data   int
	Part   int32
	Offset int64
}

func NewNumMesg(i int) *NumMesg {
	return &NumMesg{Data: i}
}

func NewCheckpointMesg(part int32, offset int64) *NumMesg {
	return &NumMesg{Part: part, Offset: offset}
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

var peercnt = flag.Int("peercnt", 1, "the expected number of peers that will take part in the grid")
var kafka = flag.String("kafka", "localhost:10092", "listof kafka brokers, for example: localhost:10092,localhost:10093")
var khosts []string

func main() {
	flag.Parse()

	khosts = strings.Split(*kafka, ",")

	kconf := grid.DefaultKafkaConfig()
	kconf.Brokers = khosts

	g, err := grid.NewWithKafkaConfig(GridName, *peercnt, kconf)
	if err != nil {
		log.Fatalf("error: example: failed to create grid: %v", err)
	}

	g.UseStateTopic("state")

	g.AddDecoder(NewNumMesgDecoder, "state", "topic1", "topic2", "topic3")
	g.AddEncoder(NewNumMesgEncoder, "state", "topic1", "topic2", "topic3")

	g.Add("add", 2, add, "topic1")
	g.Add("mul", 2, mul, "topic2")

	g.Start()

	go readline()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	g.Wait()
}

func add(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)

		// Recovery Phase I.
		for event := range in {
			switch mesg := event.Message().(type) {
			case *NumMesg:
				log.Printf("example: add(): state message: NumMesg%v", mesg)
			case grid.Ready:
				goto Offsets
			default:
			}
		}

	Offsets:
		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewWritable("", "", grid.UseOffset{Topic: mesg.Topic, Part: mesg.Part, Offset: mesg.Max})
			case grid.Ready:
				goto Recovered
			default:
			}
		}

	Recovered:
		log.Printf("example: add(): recovered")
		for event := range in {
			switch mesg := event.Message().(type) {
			case *NumMesg:
				outmsg := 1 + mesg.Data
				out <- grid.NewWritable("state", Key, NewCheckpointMesg(event.Part(), event.Offset()))
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

		// Recovery Phase I.
		for event := range in {
			switch mesg := event.Message().(type) {
			case *NumMesg:
				log.Printf("example: mul(): state message: NumMesg%v", mesg)
			case grid.Ready:
				goto Offsets
			default:
			}
		}

	Offsets:
		for event := range in {
			switch mesg := event.Message().(type) {
			case grid.MinMaxOffset:
				out <- grid.NewWritable("", "", grid.UseOffset{Topic: mesg.Topic, Part: mesg.Part, Offset: mesg.Max})
			case grid.Ready:
				goto Recovered
			default:
			}
		}

	Recovered:
		log.Printf("example: mul(): recovered")
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

func readline() {
	const topic = "topic1"

	client, err := sarama.NewClient(ConsumerName, khosts, sarama.NewClientConfig())
	if err != nil {
		log.Fatalf("fatal: example: failed to create kafka client: %v", err)
	}
	defer client.Close()

	producer, err := sarama.NewSimpleProducer(client, topic, sarama.NewHashPartitioner)
	if err != nil {
		log.Fatalf("fatal: example: topic: %v: failed to create producer: %v", topic, err)
	}
	defer producer.Close()

	for {
		var i int
		fmt.Println("example: enter a number:")
		if _, err := fmt.Scanf("%d", &i); err != nil {
			log.Printf("error: example: bad input")
		} else {
			data := struct {
				Data int
			}{
				i,
			}
			if bytes, err := json.Marshal(data); err != nil {
				log.Printf("error: example: %v", err)
			} else {
				producer.SendMessage(nil, sarama.StringEncoder(bytes))
			}
		}
	}
}
