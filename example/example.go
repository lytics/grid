package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/Shopify/sarama"
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

var peercnt = flag.Int("peercnt", 1, "the expected number of peers that will take part in the grid.")

func main() {
	flag.Parse()

	g, err := grid.New(GridName, *peercnt)
	if err != nil {
		log.Fatalf("error: example: failed to create grid: %v", err)
	}

	g.AddDecoder(NewNumMesgDecoder, "topic1", "topic2", "topic3")
	g.AddEncoder(NewNumMesgEncoder, "topic1", "topic2", "topic3")

	err = g.Add("add", 2, add, "topic1")
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}

	err = g.Add("mul", 2, mul, "topic2")
	if err != nil {
		log.Fatalf("error: example: %v", err)
	}

	g.Start()

	go consoleMessageSource()

	g.Wait()
}

//Read integers from the console and sends them into the grid's first topic.
//
//For this grid the source of the stream is a kafka topic named "topic1",
//Defined by g.Read("add", "topic1") above.
func consoleMessageSource() {
	client, err := sarama.NewClient(ConsumerName, []string{"localhost:10092"}, sarama.NewClientConfig())
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	producer, err := sarama.NewSimpleProducer(client, "topic1", sarama.NewHashPartitioner)
	if err != nil {
		log.Fatalf("error: topic: failed to create producer: %v", err)
	}
	defer producer.Close()

	for {
		var i int
		fmt.Println("Enter a number:")
		if _, err := fmt.Scanf("%d", &i); err != nil {
			log.Printf("error: bad input")
		} else {
			log.Printf("sending %d to the grid for processing.\n", i)
			data := struct {
				Data int
			}{
				i,
			}
			if bytes, err := json.Marshal(data); err != nil {
				log.Printf("error: %v", err)
			} else {
				producer.SendMessage(nil, sarama.StringEncoder(bytes))
			}
		}
	}
}

func add(in <-chan grid.Event) <-chan grid.Event {
	out := make(chan grid.Event)

	go func() {
		defer close(out)
		for e := range in {
			switch mesg := e.Message().(type) {
			case *NumMesg:
				outmsg := 1 + mesg.Data
				log.Printf("add(): in-msg=%d -> out-mgs=%d\n", mesg.Data, outmsg)
				out <- grid.NewWritable("topic2", Key, NewNumMesg(outmsg))
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
				outmsg := 2 * mesg.Data
				log.Printf("mul(): in-msg=%d -> out-mgs=%d\n", mesg.Data, outmsg)
				out <- grid.NewWritable("topic3", Key, NewNumMesg(outmsg))
			default:
				log.Printf("example: unknown message: %T :: %v", mesg, mesg)
			}
		}
	}()

	return out
}
