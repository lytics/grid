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
	kafka    *sarama.Client
	name     string
	cmdtopic string
	kconfig  *KafkaConfig
	encoders map[string]func(io.Writer) Encoder
	decoders map[string]func(io.Reader) Decoder
	wg       *sync.WaitGroup
	mutex    *sync.Mutex
	exit     chan bool
	voter    *Voter
	manager  *Manager
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

func NewWithKafkaConfig(name string, kconfig *KafkaConfig) (*Grid, error) {
	kafka, err := sarama.NewClient(kconfig.BaseName+"_shared_client", kconfig.Brokers, kconfig.ClientConfig)
	if err != nil {
		return nil, err
	}

	cmdtopic := name + "-cmd"

	g := &Grid{
		kafka:    kafka,
		kconfig:  kconfig,
		name:     name,
		cmdtopic: cmdtopic,
		encoders: make(map[string]func(io.Writer) Encoder),
		decoders: make(map[string]func(io.Reader) Decoder),
		wg:       new(sync.WaitGroup),
		mutex:    new(sync.Mutex),
		exit:     make(chan bool),
		voter:    NewVoter(0, cmdtopic, 2, 0),
		manager:  NewManager(0, cmdtopic, 3),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, cmdtopic)
	g.AddEncoder(NewCmdMesgEncoder, cmdtopic)

	return g, nil
}

func (g *Grid) Start() error {
	g.mutex.Lock()
	defer g.mutex.Unlock()

	parts, err := g.kafka.Partitions(g.cmdtopic)
	if err != nil {
		log.Fatalf("error: topic: %v: failed getting kafka partition data: %v", g.cmdtopic, err)
	}

	vin := startTopicReader(g.cmdtopic, g.kconfig, NewCmdMesgDecoder, parts)
	vout := g.voter.startStateMachine(vin, g.exit)
	startTopicWriter(g.cmdtopic, g.kafka, NewCmdMesgEncoder, vout)

	min := startTopicReader(g.cmdtopic, g.kconfig, NewCmdMesgDecoder, parts)
	mout := g.manager.startStateMachine(min, g.exit)
	startTopicWriter(g.cmdtopic, g.kafka, NewCmdMesgEncoder, mout)

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
	if _, exists := g.manager.ops[fname]; exists {
		return fmt.Errorf("gird: already added: %v", fname)
	}

	op := &op{f: f, n: n, inputs: make(map[string]bool), outputs: make(map[string]bool)}

	for _, topic := range topics {
		if _, found := g.decoders[topic]; !found {
			return fmt.Errorf("grid: no decoder added for topic: %v", topic)
		}

		op.inputs[topic] = true
	}

	g.manager.ops[fname] = op

	return nil
}

func (g *Grid) Restrict(fname, topic string, parts ...int32) error {
	if _, exists := g.manager.ops[fname]; !exists {
		return fmt.Errorf("gird: does not exist: %v: reader of: %v", fname, topic)
	}

	if !g.manager.ops[fname].inputs[topic] {
		return fmt.Errorf("gird: %v: not set as reader of: %v", fname, topic)
	}

	g.manager.ops[fname].parts[topic] = parts
	return nil
}

type op struct {
	n       int
	f       func(in <-chan Event) <-chan Event
	inputs  map[string]bool
	outputs map[string]bool
	parts   map[string][]int32
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

// func (g *Grid) startup(fname string, tslices []*topicslice) {
// 	if _, exists := g.ops[fname]; !exists {
// 		log.Fatalf("error: grid: does not exist: %v(): reader of: %v", fname, topic)
// 	}

// 	ins := make([]<-chan Event, 0)
// 	for _, tslice := range tslices {
// 		newdec, found := g.decoders[tslice.topic]
// 		if !found {
// 			log.Fatalf("error: grid: %v(): has no decoder as reader of: %v", fname, topic)
// 		}

// 		if !g.ops[fname].inputs[topic] {
// 			log.Fatalf("error: grid: %v(): not set as reader of: %v", fname, topic)
// 		}

// 		ins = append(ins, startTopicReader(topic, g.kafka, g.kconfig, newdec, tslice.parts...))
// 	}

// 	out := g.ops[fname].f(merge(ins))

// 	outs := make(map[string]chan Event)
// 	for topic, newenc := range g.encoders {
// 		outs[topic] = make(chan Event, 1024)
// 		startTopicWriter(topic, g.kafka, newenc, outs[topic])

// 		go func(fname string, out <-chan Event, outs map[string]chan Event) {
// 			for e := range out {
// 				if tchan, found := outs[e.Topic()]; found {
// 					tchan <- e
// 				} else {
// 					log.Printf("error: grid: %v(): has no encoder as writer of: %v", fname, e.Topic())
// 				}
// 			}
// 		}(fname, out, outs)
// 	}
// }
