package grid

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	metrics "github.com/rcrowley/go-metrics"
)

type Actor interface {
	Act(in <-chan Event) <-chan Event
}

type Decoder interface {
	New() interface{}
	Decode(d interface{}) error
}

type Encoder interface {
	Encode(e interface{}) error
}

type Grid struct {
	log        ReadWriteLog
	gridname   string
	cmdtopic   string
	statetopic string
	npeers     int
	quorum     uint32
	parts      map[string][]int32
	lines      map[string]*line
	wg         *sync.WaitGroup
	exit       chan bool
	registry   metrics.Registry
	// Test hook, normaly should be 0.
	maxleadertime int64
}

func DefaultKafkaConfig() *KafkaConfig {
	brokers := []string{"localhost:10092"}

	pconfig := sarama.NewProducerConfig()
	pconfig.FlushMsgCount = 10000
	pconfig.FlushFrequency = 1 * time.Second
	cconfig := sarama.NewConsumerConfig()
	cconfig.DefaultFetchSize = 512000
	cconfig.OffsetMethod = sarama.OffsetMethodNewest

	return &KafkaConfig{
		Brokers:        brokers,
		ClientConfig:   sarama.NewClientConfig(),
		ProducerConfig: pconfig,
		ConsumerConfig: cconfig,
	}
}

func New(gridname string, npeers int) (*Grid, error) {
	return NewWithKafkaConfig(gridname, npeers, DefaultKafkaConfig())
}

func NewWithKafkaConfig(gridname string, npeers int, kconfig *KafkaConfig) (*Grid, error) {
	cmdtopic := gridname + "-cmd"

	kconfig.cmdtopic = cmdtopic
	kconfig.basename = gridname

	rwlog, err := NewKafkaReadWriteLog(buildPeerName(0), kconfig)
	if err != nil {
		return nil, err
	}

	g := &Grid{
		log:      rwlog,
		gridname: gridname,
		cmdtopic: cmdtopic,
		npeers:   npeers,
		quorum:   uint32((npeers / 2) + 1),
		parts:    make(map[string][]int32),
		lines:    make(map[string]*line),
		wg:       new(sync.WaitGroup),
		exit:     make(chan bool),
	}

	g.wg.Add(1)

	g.AddDecoder(NewCmdMesgDecoder, cmdtopic)
	g.AddEncoder(NewCmdMesgEncoder, cmdtopic)

	return g, nil
}

func (g *Grid) Start() error {
	if g.registry == nil {
		g.registry = metrics.DefaultRegistry
	}

	voter := NewVoter(0, g)
	manager := NewManager(0, g)

	// Why are these both read-only channels?
	// Because:
	//  * The reader's output is our input.
	//  * Our output is the writer's input.
	var in <-chan Event
	var out <-chan Event

	// Command topic only uses the 0 partition
	// to make sure all communication has a
	// total ordering.

	_, max, err := g.log.Offsets(g.cmdtopic, 0)
	if err != nil {
		log.Fatalf("fatal: grid: topic: %v: failed to get offset for partition: %v: %v", g.cmdtopic, 0, err)
	}

	// Read needs to know topic, partitions, offsets, and
	// an exit channel for early exits.

	cmdpart := []int32{0}
	cmdoffset := []int64{max}

	in = g.log.Read(g.cmdtopic, cmdpart, cmdoffset, g.exit)
	out = voter.startStateMachine(in)
	g.log.Write(g.cmdtopic, out)

	in = g.log.Read(g.cmdtopic, cmdpart, cmdoffset, g.exit)
	out = manager.startStateMachine(in)
	g.log.Write(g.cmdtopic, out)

	return nil
}

func (g *Grid) Wait() {
	g.wg.Wait()
}

func (g *Grid) Stop() {
	close(g.exit)
	g.wg.Done()
}

func (g *Grid) UseStateTopic(topic string) {
	g.statetopic = topic
}

func (g *Grid) UseMetrics(registry metrics.Registry) {
	g.registry = registry
}

func (g *Grid) AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string) {
	g.log.AddDecoder(makeDecoder, topics...)
}

func (g *Grid) AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string) {
	g.log.AddEncoder(makeEncoder, topics...)
}

func (g *Grid) AddPartitioner(p func(key io.Reader, parts int32) int32, topics ...string) {
	g.log.AddPartitioner(p, topics...)
}

func (g *Grid) Add(name string, n int, a Actor, topics ...string) error {
	if _, exists := g.lines[name]; exists {
		return fmt.Errorf("gird: already added: %v", name)
	}

	line := &line{a: a, n: n, inputs: make(map[string]bool)}

	for _, topic := range topics {
		if _, found := g.log.DecodedTopics()[topic]; !found {
			return fmt.Errorf("grid: topic: %v: no decoder found for topic", topic)
		}
		line.inputs[topic] = true

		// Discover the available partitions for the topic.
		parts, err := g.log.Partitions(topic)
		if err != nil {
			return fmt.Errorf("grid: topic: %v: failed getting partition data: %v", topic, err)
		}
		g.parts[topic] = parts

		if len(parts) < n {
			return fmt.Errorf("grid: topic: %v: parallelism of function is greater than number of partitions: func: %v, parallelism: %d partitions: %d", topic, name, n, len(parts))
		}
	}

	g.lines[name] = line

	return nil
}

// startinst starts an instance of a function which will process messages from its input topics
// and possibly write messages to output topics.
//
// The high level points of starting an instance are:
//
//     1. Create the function's input channel, events read from
//        Kafka are written to this channel.
//
//     2. Start the function, passing in the input topic, and
//        getting back the function's output channel.
//
//     3. If a state topic is specified, first all messages
//        from this topic need to be read and fed into the
//        instance, that is the guarantee of the state topic,
//        that the first events the instance gets our its
//        state events, so that it can rebuild its state
//        before dealing with the real input. The state topic
//        can be considered a WAL: Write Ahead Log.
//
//     4. Next discover the min and max offset available in
//        Kafka of each topic-partition that the instance
//        specifies as an input topic, and send this min and
//        max information to the instance.
//
//     5. Wait to get messages from the instance which specify
//        which offset to use for each topic-partition.
//
//     6. With requested offsets in hand, connect the instance
//        to it's actual input topics at the offsets requested.
//
//     7. Connect the instance to the topic writers, so that
//        the instance can write data.
//
func (g *Grid) startinst(inst *Instance) {
	fname := inst.Fname
	id := inst.Id

	// Validate early that the instance was added to the grid.
	if _, exists := g.lines[fname]; !exists {
		log.Fatalf("fatal: grid: does not exist: %v()", fname)
	}

	// Validate early that the instance has valid topics and partition subsets.
	for topic, _ := range inst.TopicSlices {
		if !g.lines[fname].inputs[topic] {
			log.Fatalf("fatal: grid: %v(): not set as reader of: %v", fname, topic)
		}
	}

	// The in channel will be used by this instance to receive data, ie: its input.
	in := make(chan Event)

	// The out channel will be used by this instance to transmit data, ie: its output.
	out := g.lines[fname].a.Act(in)

	// Recover previous state if the grid sepcifies a state topic.
	if "" != g.statetopic {
		parts, err := g.log.Partitions(g.statetopic)
		if err != nil {
			log.Fatalf("fatal: grid: %v: instance: %v: topic: %v: failed to get partition data: %v", fname, id, g.statetopic, err)
		}

		// Here we get the min and max offset of the state topic.
		// This is used below to determin if there is anything
		// in the topic.
		maxs := make([]int64, len(parts))
		mins := make([]int64, len(parts))
		for _, part := range parts {
			min, max, err := g.log.Offsets(g.statetopic, part)
			if err != nil {
				log.Fatalf("fatal: grid: %v: instance: %v: topic: %v: part: %v: failed to get offset: %v", fname, id, g.statetopic, part, err)
			}
			maxs[part] = max
			mins[part] = min
		}

		// If there is a difference between any partition's
		// min and max, then we have state events to read.
		var readstate bool
		for i, max := range maxs {
			if mins[i] != max {
				readstate = true
			}
		}

		// If there are state events, use a reader to read them
		// and pump them into the instance, so it can recover
		// its state.
		if readstate {
			exit := make(chan bool)
			for event := range g.log.Read(g.statetopic, parts, mins, exit) {
				if event.Offset()+1 >= maxs[event.Part()] {
					in <- NewReadable(g.gridname, 0, 0, Ready(true))
					close(exit)
				} else {
					in <- event
				}
			}
		}
	}

	cnt := 0
	useoffsets := make(map[string]map[int32]int64)

	// Initialize to track which offsets for each topic and partition
	// the instance will want to use.
	for topic, parts := range inst.TopicSlices {
		cnt += len(parts)
		useoffsets[topic] = make(map[int32]int64)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)

	// For each topic-partition the instance is setup to read from,
	// offer it the available min and max.
	go func() {
		defer wg.Done()
		for topic, parts := range inst.TopicSlices {
			for _, part := range parts {
				min, max, err := g.log.Offsets(topic, part)
				if err != nil {
					log.Fatalf("fatal: grid: %v: instance: %v: topic: %v: partition: %v: failed getting offset: %v", fname, id, topic, part, err)
				}
				in <- NewReadable(g.gridname, 0, 0, MinMaxOffset{Topic: topic, Part: part, Min: min, Max: max})
			}
		}
	}()

	// At the same time expect that the instance will respond with a use-offset
	// for each min-max topic-partition pair sent to it.
	go func() {
		defer wg.Done()

		if 0 == len(inst.TopicSlices) {
			log.Printf("grid: %v: instance: %v: is a source and has no input topics", fname, id)
			return
		}

		for event := range out {
			switch msg := event.Message().(type) {
			case UseOffset:
				useoffsets[msg.Topic][msg.Part] = msg.Offset
				cnt--
			default:
			}
			if cnt == 0 {
				in <- NewReadable(g.gridname, 0, 0, Ready(true))
				return
			}
		}
	}()

	wg.Wait()

	// Start the true topic reading. The messages on the input channels are
	// mux'ed and put onto a single merged input channel.
	for topic, parts := range inst.TopicSlices {
		go func() {
			offsets := make([]int64, len(parts))
			for i, part := range parts {
				if offset, found := useoffsets[topic][part]; !found {
					log.Fatalf("fatal: grid: %v: instance: %v: failed to find offset for topic: %v: partition: %v", fname, id, topic, part)
				} else {
					offsets[i] = offset
				}
			}

			log.Printf("grid: starting: %v: instance: %v: topic: %v: partitions: %v: offsets: %v", fname, id, topic, parts, offsets)
			for event := range g.log.Read(topic, parts, offsets, g.exit) {
				in <- event
			}
		}()
	}

	// Create a mapping of all output topics defined which the instance
	// may write to before we finish connecting the instance to its
	// output channel.
	outs := make(map[string]chan Event)
	for topic, _ := range g.log.EncodedTopics() {
		outs[topic] = make(chan Event, 1024)
	}

	// Start the true topic writing. The messages on the output channel are
	// de-mux'ed and put on topic specific output channels.
	for topic, _ := range g.log.EncodedTopics() {
		g.log.Write(topic, outs[topic])
		go func() {
			for event := range out {
				if topicout, found := outs[event.Topic()]; found {
					topicout <- event
				} else {
					log.Fatalf("fatal: grid: %v: instance: %v: no encoder found for topic: %v", fname, id, event.Topic())
				}
			}
		}()
	}
}

// line is a line of actors in the grid. The name
// is a bit jokey though, ie: "grid line"
type line struct {
	n      int
	a      Actor
	inputs map[string]bool
}
