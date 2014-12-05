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
	kafka         *sarama.Client
	kconfig       *KafkaConfig
	gridname      string
	cmdtopic      string
	npeers        int
	quorum        uint32
	maxleadertime int64
	encoders      map[string]func(io.Writer) Encoder
	decoders      map[string]func(io.Reader) Decoder
	parts         map[string][]int32
	ops           map[string]*op
	wg            *sync.WaitGroup
	exit          chan bool
}

func New(gridname string, npeers int) (*Grid, error) {

	brokers := []string{"localhost:10092"}

	pconfig := sarama.NewProducerConfig()
	cconfig := sarama.NewConsumerConfig()
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	kafkaClientConfig := &KafkaConfig{
		Brokers:        brokers,
		BaseName:       gridname,
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
	}

	return NewWithKafkaConfig(gridname, npeers, kafkaClientConfig)
}

func NewWithKafkaConfig(gridname string, npeers int, kconfig *KafkaConfig) (*Grid, error) {
	kafka, err := sarama.NewClient(kconfig.BaseName+"_shared_client", kconfig.Brokers, kconfig.ClientConfig)
	if err != nil {
		return nil, err
	}

	cmdtopic := gridname + "-cmd"

	g := &Grid{
		kafka:    kafka,
		kconfig:  kconfig,
		gridname: gridname,
		cmdtopic: cmdtopic,
		npeers:   npeers,
		quorum:   uint32((npeers / 2) + 1),
		encoders: make(map[string]func(io.Writer) Encoder),
		decoders: make(map[string]func(io.Reader) Decoder),
		parts:    make(map[string][]int32),
		ops:      make(map[string]*op),
		wg:       new(sync.WaitGroup),
		exit:     make(chan bool),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, cmdtopic)
	g.AddEncoder(NewCmdMesgEncoder, cmdtopic)

	return g, nil
}

func (g *Grid) Start() error {

	parts, err := g.kafka.Partitions(g.cmdtopic)
	if err != nil {
		log.Fatalf("error: topic: %v: failed getting kafka partition data: %v", g.cmdtopic, err)
	}

	voter := NewVoter(0, g)
	manager := NewManager(0, g)

	var in <-chan Event
	var out <-chan Event

	in = startTopicReader(g.cmdtopic, g.kconfig, NewCmdMesgDecoder, parts)
	out = voter.startStateMachine(in)
	startTopicWriter(g.cmdtopic, g.kafka, NewCmdMesgEncoder, out)

	in = startTopicReader(g.cmdtopic, g.kconfig, NewCmdMesgDecoder, parts)
	out = manager.startStateMachine(in)
	startTopicWriter(g.cmdtopic, g.kafka, NewCmdMesgEncoder, out)

	return nil
}

func (g *Grid) Wait() {
	g.wg.Wait()
}

func (g *Grid) Stop() {
	close(g.exit)
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

func (g *Grid) Add(fgridname string, n int, f func(in <-chan Event) <-chan Event, topics ...string) error {
	if _, exists := g.ops[fgridname]; exists {
		return fmt.Errorf("gird: already added: %v", fgridname)
	}

	op := &op{f: f, n: n, inputs: make(map[string]bool)}

	for _, topic := range topics {
		if _, found := g.decoders[topic]; !found {
			return fmt.Errorf("grid: no decoder added for topic: %v", topic)
		}

		parts, err := g.kafka.Partitions(topic)
		if err != nil {
			log.Fatalf("error: topic: %v: failed getting kafka partition data: %v", g.cmdtopic, err)
		}
		g.parts[topic] = parts

		op.inputs[topic] = true
	}

	g.ops[fgridname] = op

	return nil
}

type op struct {
	n      int
	f      func(in <-chan Event) <-chan Event
	inputs map[string]bool
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

// func (g *Grid) startup(fgridname string, tslices []*topicslice) {
// 	if _, exists := g.ops[fgridname]; !exists {
// 		log.Fatalf("error: grid: does not exist: %v(): reader of: %v", fgridname, topic)
// 	}

// 	ins := make([]<-chan Event, 0)
// 	for _, tslice := range tslices {
// 		newdec, found := g.decoders[tslice.topic]
// 		if !found {
// 			log.Fatalf("error: grid: %v(): has no decoder as reader of: %v", fgridname, topic)
// 		}

// 		if !g.ops[fgridname].inputs[topic] {
// 			log.Fatalf("error: grid: %v(): not set as reader of: %v", fgridname, topic)
// 		}

// 		ins = append(ins, startTopicReader(topic, g.kafka, g.kconfig, newdec, tslice.parts...))
// 	}

// 	out := g.ops[fgridname].f(merge(ins))

// 	outs := make(map[string]chan Event)
// 	for topic, newenc := range g.encoders {
// 		outs[topic] = make(chan Event, 1024)
// 		startTopicWriter(topic, g.kafka, newenc, outs[topic])

// 		go func(fgridname string, out <-chan Event, outs map[string]chan Event) {
// 			for e := range out {
// 				if tchan, found := outs[e.Topic()]; found {
// 					tchan <- e
// 				} else {
// 					log.Printf("error: grid: %v(): has no encoder as writer of: %v", fgridname, e.Topic())
// 				}
// 			}
// 		}(fgridname, out, outs)
// 	}
// }
