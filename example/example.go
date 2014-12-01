package main

import (
	"encoding/json"
	"io"
	"log"

	"github.com/Shopify/sarama"
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

	go consoleMessageSource()

	g.Wait()
}

//Read integers from the console and sends them into the grid's first topic.
//
//For this grid the source of the stream is a kafka topic named "topic1",
//Defined by g.Read("add", "topic1") above.
func consoleMessageSource() {
	client, err := sarama.NewClient("consoleMessage-client", []string{"localhost:10092"}, sarama.NewClientConfig())
	if err != nil {
		t.Fatalf("failed to create kafka client: %v", err)
	}
	producer, err := sarama.NewSimpleProducer(client, "topic1", sarama.NewHashPartitioner)
	if err != nil {
		log.Fatalf("error: topic: failed to create producer: %v", err)
	}
	defer producer.Close()

	for {
		var i int
		fmt.Println("Enter a number please:")
		if _, err := fmt.Scanf("%d", &i); err != nil {
			fmt.Println("Your input was invalid!")
		} else {
			fmt.Printf("Sending %d to the grid for processing.\n", i)

			if bytes, err := json.Marshal(i); err != nil {
				fmt.Println("error:", err)
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
				out <- grid.NewWritable("topic2", Key, NewNumMesg(1+mesg.Data))
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
				out <- grid.NewWritable("topic3", Key, NewNumMesg(2*mesg.Data))
			default:
				log.Printf("example: unknown message: %T :: %v", mesg, mesg)
			}
		}
	}()

	return out
}
