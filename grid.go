package grid

import (
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

	for fname, op := range g.ops {
		log.Printf("grid: starting %v() <%v >%v", fname, op.inputs, op.outputs)

		ins := make([]<-chan Event, 0)
		for _, topic := range op.inputs {
			newdec, found := g.decoders[topic]
			if !found {
				log.Fatalf("error: grid: %v reader of: %v, but no decoder added", fname, topic)
			}
			ins = append(ins, StartTopicReader(topic, g.kafka, newdec))
		}

		out := op.f(merge(ins))

		for _, topic := range op.outputs {
			newenc, found := g.encoders[topic]
			if !found {
				log.Fatalf("error: grid: %v writer of: %v, but no encoder added", fname, topic)
			}
			StartTopicWriter(topic, g.kafka, newenc, out)
		}
	}

	return nil
}

func merge(ins []<-chan Event) <-chan Event {
	merged := make(chan Event, 0)
	for _, in := range ins {
		go func(in <-chan Event) {
			for m := range in {
				merged <- m
			}
		}(in)
	}
	return merged
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

type op struct {
	n       int
	f       func(in <-chan Event) <-chan Event
	inputs  []string
	outputs []string
}

func (g *Grid) cmdTopicChannels(in <-chan *CmdMesg) (<-chan *CmdMesg, error) {
	topic := fmt.Sprintf("%v-cmd", g.name)

	kout := make(chan Event)
	kin := StartTopicReader(topic, g.kafka, g.decoders[topic])
	StartTopicWriter(topic, g.kafka, g.encoders[topic], kout)

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
			kout <- NewWritable("", vm)
		}
	}()

	return out, nil
}
