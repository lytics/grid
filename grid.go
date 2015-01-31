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

type Grid struct {
	log        ReadWriteLog
	gridname   string
	cmdtopic   string
	statetopic string
	npeers     int
	quorum     uint32
	parts      map[string][]int32
	actorconfs map[string]*actorconf
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
		log:        rwlog,
		gridname:   gridname,
		cmdtopic:   cmdtopic,
		npeers:     npeers,
		quorum:     uint32((npeers / 2) + 1),
		parts:      make(map[string][]int32),
		actorconfs: make(map[string]*actorconf),
		wg:         new(sync.WaitGroup),
		exit:       make(chan bool),
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

func (g *Grid) AddPartitioner(p Partitioner, topics ...string) {
	g.log.AddPartitioner(p, topics...)
}

func (g *Grid) Add(name string, n int, build NewActor, topics ...string) error {
	if _, exists := g.actorconfs[name]; exists {
		return fmt.Errorf("actor: %v: already added", name)
	}

	actorconf := &actorconf{build: build, n: n, inputs: make(map[string]bool)}

	for _, topic := range topics {
		// Check that the actors intput topics all have decoders.
		if _, found := g.log.DecodedTopics()[topic]; !found {
			return fmt.Errorf("actor: %v: topic: %v: no decoder found for topic", name, topic)
		}
		actorconf.inputs[topic] = true

		// Discover the available partitions for the topic.
		parts, err := g.log.Partitions(topic)
		if err != nil {
			return fmt.Errorf("actor: %v: topic: %v: failed getting partition data: %v", name, topic, err)
		}
		g.parts[topic] = parts

		// Max parallelism is set by max partition count.
		if len(parts) < n && topic != g.statetopic {
			return fmt.Errorf("actor: %v: topic: %v: number of actors: %d is greater than number of partitions: %d", name, topic, n, len(parts))
		}
	}

	g.actorconfs[name] = actorconf

	return nil
}

// startInstance starts one actor instance. The basic steps of doing this:
//
//     1. Create the "in" and "state" channels.
//     2. Build the actor and call its Act method.
//     3. Negotiate offsets for input topics.
//     4. Connect its output channel to the output topics.
//
func (g *Grid) startInstance(inst *Instance) {
	name := inst.Name
	id := inst.Id

	// Validate early that the instance was added to the grid.
	if _, exists := g.actorconfs[name]; !exists {
		log.Fatalf("fatal: grid: actor: %v: never configured", name)
	}

	// Validate early that the instance has valid topics and partition subsets.
	for topic, _ := range inst.TopicSlices {
		if !g.actorconfs[name].inputs[topic] {
			log.Fatalf("fatal: grid: actor: %v: not set as reader of: %v", name, topic)
		}
	}

	// The in and state channel will be used by this instance to receive data.
	in := make(chan Event)
	state := make(chan Event)
	// Start the actor, and receive its out channel.
	out := g.actorconfs[name].build(name, id).Act(in, state)

	// Start the true topic reading. The messages on the input channels are
	// mux'ed and put onto a single merged input channel.
	for topic, parts := range inst.TopicSlices {
		go g.negotiateReadOffsets(id, name, topic, parts, in)
	}

	// Create a mapping of all output topics defined which the instance
	// may write to before connecting the instance to its out channel.
	outs := make(map[string]chan Event)
	for topic, _ := range g.log.EncodedTopics() {
		outs[topic] = make(chan Event, 1024)
	}

	// Start the true topic writing. The messages on the output channel are
	// de-mux'ed and put on topic-specific output channels.
	for topic, _ := range g.log.EncodedTopics() {
		g.log.Write(topic, outs[topic])
		go func() {
			for event := range out {
				if topicout, found := outs[event.Topic()]; found {
					topicout <- event
				} else {
					// If this is ever logged its a bug on Grid's part.
					log.Fatalf("fatal: grid: actor: %v: instance: %v: no encoder found for topic: %v", name, id, event.Topic())
				}
			}
		}()
	}
}

// negotiateReadOffsets sends min max offset information for its topic
// to its actor. When the actor has responded with all the needed
// offset choices, reading of those topic parts is started.
func (g *Grid) negotiateReadOffsets(id int, name, topic string, parts []int32, state chan<- Event) {

	response := make(chan *useoffset)
	defer close(response)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	needed := make(map[int32]int64)
	for {
		select {
		case <-ticker.C:
			for _, part := range parts {
				min, max, err := g.log.Offsets(topic, part)
				if err != nil {
					log.Fatalf("fatal: grid: %v: instance: %v: topic: %v: partition: %v: failed getting offset: %v", name, id, topic, part, err)
				}
				state <- NewReadable(g.gridname, 0, 0, NewMinMaxOffset(min, max, part, topic, response))
			}
		case use := <-response:
			needed[use.Part] = use.Offset
			if len(needed) == len(parts) {
				offsets := make([]int64, 0, len(parts))
				for i, part := range parts {
					offsets[i] = needed[part]
				}
				log.Printf("grid: starting reader for: %v: instance: %v: topic: %v: partitions: %v: offsets: %v", name, id, topic, parts, offsets)
				if topic == g.statetopic {
					go func(topic string, parts []int32, offsets []int64) {
						for event := range g.log.Read(topic, parts, offsets, g.exit) {
							state <- event
						}
					}(topic, parts, offsets)
				} else {
					go func(topic string, parts []int32, offsets []int64) {
						for event := range g.log.Read(topic, parts, offsets, g.exit) {
							state <- event
						}
					}(topic, parts, offsets)
				}
				return
			}
		}
	}
}

// actorconf is a configuration for a set actors in the grid.
type actorconf struct {
	n      int
	build  NewActor
	inputs map[string]bool
}
