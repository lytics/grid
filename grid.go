package grid

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

type Decoder interface {
	New() interface{}
	Decode(d interface{}) error
}

type Encoder interface {
	Encode(e interface{}) error
}

type Grid struct {
	kafka     *sarama.Client
	name      string
	pconfig   *sarama.ProducerConfig
	cconfig   *sarama.ConsumerConfig
	consumers []*sarama.Consumer
	producers []*sarama.Producer
	encoders  map[string]func(io.Writer) Encoder
	decoders  map[string]func(io.Reader) Decoder
	ops       map[string]*op
	wg        *sync.WaitGroup
	mutex     *sync.Mutex
}

func New(name string) (*Grid, error) {
	kafka, err := sarama.NewClient(name, []string{"localhost:10092"}, sarama.NewClientConfig())
	if err != nil {
		return nil, err
	}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	g := &Grid{
		kafka:     kafka,
		name:      name,
		pconfig:   pconfig,
		cconfig:   cconfig,
		consumers: make([]*sarama.Consumer, 0),
		encoders:  make(map[string]func(io.Writer) Encoder),
		decoders:  make(map[string]func(io.Reader) Decoder),
		ops:       make(map[string]*op),
		wg:        new(sync.WaitGroup),
		mutex:     new(sync.Mutex),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, fmt.Sprintf("%v-cmd", name))
	g.AddEncoder(NewCmdMesgEncoder, fmt.Sprintf("%v-cmd", name))

	return g, nil
}

func (g *Grid) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	// go func() {
	// 	out := make(chan *CmdMesg)
	// 	defer close(out)

	// 	in, err := g.cmdTopicChannels(out)
	// 	if err != nil {
	// 		return
	// 	}

	// 	voter(0, 1, 0, in, out)
	// }()

	for fname, op := range g.ops {
		log.Printf("grid: starting %v() <%v >%v", fname, op.inputs, op.outputs)
		in := g.reader(fname, op.inputs...)
		go func(fname string, g *Grid, in <-chan Event, f func(<-chan Event) <-chan Event) {
			g.writer(fname, f(in), op.outputs...)
		}(fname, g, in, op.f)
	}
	return nil
}

func (g *Grid) Wait() {
	g.wg.Wait()
}

func (g *Grid) Stop() {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.consumers == nil {
		return
	}

	log.Printf("grid: shutting down")
	for _, consumer := range g.consumers {
		consumer.Close()
	}
	g.consumers = nil
	g.wg.Done()
}

func (g *Grid) AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string) {
	for _, topic := range topics {
		// Only add the decoder if it has not been added before, this is
		// used to register certain decoders before the user can.
		if _, added := g.decoders[topic]; !added {
			g.decoders[topic] = makeDecoder
		}
	}
}

func (g *Grid) AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string) {
	for _, topic := range topics {
		// Only add the encoder if it has not been added before, this is
		// used to register certain encoders before the user can.
		if _, added := g.encoders[topic]; !added {
			g.encoders[topic] = makeEncoder
		}
	}
}

func (g *Grid) Add(fname string, n int, f func(in <-chan Event) <-chan Event) error {
	if _, exists := g.ops[fname]; exists {
		return fmt.Errorf("gird: already added: %v", fname)
	}

	g.ops[fname] = &op{f: f, n: n, inputs: make([]string, 0), outputs: make([]string, 0)}
	return nil
}

func (g *Grid) Read(fname, topic string) error {
	if op, exists := g.ops[fname]; exists {
		op.inputs = append(op.inputs, topic)
		return nil
	} else {
		return fmt.Errorf("grid: no such name: %v", fname)
	}
}

func (g *Grid) Write(fname, topic string) error {
	if op, exists := g.ops[fname]; exists {
		op.outputs = append(op.outputs, topic)
		return nil
	} else {
		return fmt.Errorf("grid: no such name: %v", fname)
	}
}

func (g *Grid) cmdTopicChannels(in <-chan *CmdMesg) (<-chan *CmdMesg, error) {
	topic := fmt.Sprintf("%v-cmd", g.name)

	kout := make(chan Event)
	kin := g.reader("voter", topic)
	g.writer("voter", kout, topic)

	out := make(chan *CmdMesg)
	go func() {
		defer close(out)
		for e := range kin {
			out <- e.Message().(*CmdMesg)
		}
	}()

	go func() {
		defer close(kout)
		for vm := range in {
			kout <- NewWritable(topic, "", vm)
		}
	}()

	return out, nil
}

func (g *Grid) reader(fname string, topics ...string) <-chan Event {

	// Consumers read from the real topic and push data
	// into the out channel.
	out := make(chan Event, 0)

	// Setup a wait group so that the out channel
	// can be closed when all consumers have
	// exited.
	wg := new(sync.WaitGroup)

	for _, topic := range topics {
		parts, err := g.kafka.Partitions(topic)
		if err != nil {
			log.Fatalf("error: grid: %v: topic: %v: failed getting kafka partition data: %v", topic, err)
		}

		wg.Add(len(parts))

		for _, part := range parts {
			go func(g *Grid, wg *sync.WaitGroup, part int32, topic string, out chan<- Event) {
				defer wg.Done()

				var buf bytes.Buffer

				consumer, err := g.readfrom(fname, topic, part)
				if err != nil {
					log.Fatalf("error: grid: %v consumer: %v", fname, err)
				}

				dec := g.decoders[topic](&buf)
				for e := range consumer.Events() {
					val := dec.New()
					buf.Write(e.Value)
					err = dec.Decode(val)
					if err != nil {
						log.Printf("error: grid: decode failed: %v: value: %v", err, buf.Bytes())
						buf.Reset()
					} else {
						out <- NewReadable(e.Offset, val)
					}
				}
			}(g, wg, part, topic, out)
		}
	}

	// When the kafka consumers have exited, it means there is
	// no goroutine which can write to the out channel, so
	// close it.
	go func(wg *sync.WaitGroup, out chan<- Event) {
		wg.Wait()
		close(out)
	}(wg, out)

	// The out channel is returned as a read only channel
	// so no one can close it except this code.
	return out
}

func (g *Grid) writer(fname string, in <-chan Event, topics ...string) {

	c := make([]string, len(topics))
	copy(c, topics)

	go func(in <-chan Event, topics []string) {
		producers := make(map[string]*sarama.SimpleProducer)

		for _, topic := range topics {
			pro, err := sarama.NewSimpleProducer(g.kafka, topic, sarama.NewHashPartitioner)
			defer pro.Close()
			if err != nil {
				log.Fatalf("error: grid: %v producer: %v", fname, err)
			}
			producers[topic] = pro
		}

		var buf bytes.Buffer

		// The producer is not tied to one topic like the consumers are
		// so an encoder is needed for ever registered topic.
		encoders := make(map[string]Encoder)
		for topic, newEncoder := range g.encoders {
			encoders[topic] = newEncoder(&buf)
		}

		for e := range in {
			enc, found := encoders[e.Topic()]
			if !found {
				log.Fatalf("error: grid: %v: topic not set for encoding: %v", fname, e.Topic())
			}

			pro, found := producers[e.Topic()]
			if !found {
				log.Fatalf("error: grid: %v: topic not set for output: %v, producers: %v", fname, e.Topic(), producers)
			}

			err := enc.Encode(e.Message())
			if err != nil {
				log.Printf("error: grid: %v: encode failed: %v: message: %v", fname, err, e.Message())
				buf.Reset()
			} else {
				key := []byte(e.Key())
				val := make([]byte, buf.Len())
				buf.Read(val)
				log.Printf("grid: %v sending: %v", fname, val)
				pro.SendMessage(encodable(key), encodable(val))
			}
		}
	}(in, c)
}

func (g *Grid) readfrom(fname, topic string, part int32) (*sarama.Consumer, error) {
	if topic == "" {
		return nil, fmt.Errorf("grid: topic name cannot be empty")
	}
	if part < 0 {
		return nil, fmt.Errorf("grid: partition number cannnot be negative")
	}

	group := fmt.Sprintf("%v-%v-%v", g.name, fname, topic)
	consumer, err := sarama.NewConsumer(g.kafka, topic, part, group, g.cconfig)
	if err != nil {
		return nil, err
	}
	g.consumers = append(g.consumers, consumer)
	return consumer, nil
}

type op struct {
	n       int
	f       func(in <-chan Event) <-chan Event
	inputs  []string
	outputs []string
}

// encodable is a wrapper for byte arrays to satisfy Sarama's
// crappy encoder interface. Crappy because it does not match
// the interface used by all the encoders in the standard
// packege "encoding/..."
type encodable []byte

func (e encodable) Encode() ([]byte, error) {
	return e, nil
}

func (e encodable) Length() int {
	return len(e)
}
