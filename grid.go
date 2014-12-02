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

type KafkaConfig struct {
	Brokers        []string
	BaseName       string
	ClientConfig   *sarama.ClientConfig
	ProducerConfig *sarama.ProducerConfig
	ConsumerConfig *sarama.ConsumerConfig
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
	exit      chan bool
}

func New(name string) (*Grid, error) {

	brokers := []string{"localhost:10092"}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	kafkaClientConfig := &KafkaConfig{
		Brokers:        brokers,
		BaseName:       name,
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
	}

	return NewWithKafkaConfig(name, kafkaClientConfig)
}

func NewWithKafkaConfig(name string, kafkaClientConfig *KafkaConfig) (*Grid, error) {
	kafka, err := sarama.NewClient(kafkaClientConfig.BaseName+"_shared_client", kafkaClientConfig.Brokers, kafkaClientConfig.ClientConfig)
	if err != nil {
		return nil, err
	}

	g := &Grid{
		kafka:         kafka,
		kClientConfig: kafkaClientConfig,
		name:          name,
		cmdtopic: fmt.Sprintf("%v-cmd", name),
		pconfig:       kafkaClientConfig.ProducerConfig,
		cconfig:       kafkaClientConfig.ConsumerConfig,
		consumers:     make([]*sarama.Consumer, 0),
		encoders:      make(map[string]func(io.Writer) Encoder),
		decoders:      make(map[string]func(io.Reader) Decoder),
		ops:           make(map[string]*op),
		wg:            new(sync.WaitGroup),
		mutex:         new(sync.Mutex),
		exit:     make(chan bool),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, g.cmdtopic)
	g.AddEncoder(NewCmdMesgEncoder, g.cmdtopic)

	return g, nil
}

func (g *Grid) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	in := startTopicReader(g.cmdtopic, g.kafka, g.kClientConfig, NewCmdMesgDecoder)
	out := startVoter(0, g.cmdtopic, 1, 0, in, g.exit)
	startTopicWriter(g.cmdtopic, g.kafka, NewCmdMesgEncoder, out)

	for fname, op := range g.ops {
		log.Printf("grid: starting %v() <%v >%v", fname, op.inputs, op.outputs)

		ins := make([]<-chan Event, 0)
		for _, topic := range op.inputs {
			newdec, found := g.decoders[topic]
			if found {
				ins = append(ins, startTopicReader(topic, g.kafka, g.kClientConfig, newdec))
			} else {
				log.Printf("error: grid: %v() reader of: '%v', no decoder", fname, topic)
			}
		}

		out := op.f(merge(ins))
		outs := make(map[string]chan Event)

		for topic, newenc := range g.encoders {
			outs[topic] = make(chan Event, 1024)
			startTopicWriter(topic, g.kafka, newenc, outs[topic])

			go func(fname string, out <-chan Event, outs map[string]chan Event) {
				for e := range out {
					if tchan, found := outs[e.Topic()]; found {
						tchan <- e
					} else {
						log.Printf("error: grid: %v() writer of: '%v', no encoder", fname, e.Topic())
					}
				}
			}(fname, out, outs)
		}
	}

	return nil
}

func (g *Grid) Wait() {
	g.wg.Wait()
}

func (g *Grid) Stop() {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	if g.exit == nil {
		return
	}

	close(g.exit)
	g.exit = nil

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

func (g *Grid) Add(fname string, n int, f func(in <-chan Event) <-chan Event, topics ...string) error {
	if _, exists := g.ops[fname]; exists {
		return fmt.Errorf("gird: already added: %v", fname)
	}

	op := &op{f: f, n: n, inputs: make([]string, 0), outputs: make([]string, 0)}

	for _, topic := range topics {
		if _, found := g.decoders[topic]; !found {
			return fmt.Errorf("grid: no decoder added for topic: %v", topic)
		}

		op.inputs = append(op.inputs, topic)
	}

	g.ops[fname] = op

	return nil
}

type op struct {
	n       int
	f       func(in <-chan Event) <-chan Event
	inputs  []string
	outputs []string
}

func merge(ins []<-chan Event) <-chan Event {
	merged := make(chan Event, 1024)
	for _, in := range ins {
		go func(in <-chan Event) {
			for m := range in {
				merged <- m
			}
		}(in)
	}
	return merged
}
