package grid

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Partitioner interface {
	Partition(key []byte, parts int32) int32
}

type specificpartitioner struct{}

func NewSpecificPartitioner() Partitioner {
	return &specificpartitioner{}
}

func NewSpecificPartKey(p int32) []byte {
	b := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(b, int64(p))
	if n <= 0 {
		panic("failed to create specific partition key")
	}
	return b[:n]
}

func (s *specificpartitioner) Partition(key []byte, parts int32) int32 {
	num, n := binary.Varint(key)
	if n <= 0 {
		return 0
	}
	return int32(num)
}

type ReadWriteLog interface {
	Write(topic string, in <-chan Event)
	Read(topic string, parts []int32, offsets []int64, exit <-chan bool) <-chan Event
	AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string)
	AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string)
	AddPartitioner(p Partitioner, topics ...string)
	EncodedTopics() map[string]bool
	DecodedTopics() map[string]bool
	Partitions(topic string) ([]int32, error)
	Offsets(topic string, part int32) (int64, int64, error)
}

type KafkaConfig struct {
	Brokers  []string
	Config   *sarama.Config
	cmdtopic string
	basename string
}

type kafkalog struct {
	conf         *KafkaConfig
	client       sarama.Client
	encoders     map[string]func(io.Writer) Encoder
	decoders     map[string]func(io.Reader) Decoder
	partitioners map[string]Partitioner
}

func NewKafkaReadWriteLog(id string, conf *KafkaConfig) (ReadWriteLog, error) {
	client, err := sarama.NewClient(conf.Brokers, conf.Config)
	if err != nil {
		return nil, err
	}

	return &kafkalog{
		conf:         conf,
		client:       client,
		encoders:     make(map[string]func(io.Writer) Encoder),
		decoders:     make(map[string]func(io.Reader) Decoder),
		partitioners: make(map[string]Partitioner),
	}, nil
}

func (kl *kafkalog) AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string) {
	for _, topic := range topics {
		// Only add the decoder if it has not been added before, this is
		// used to register certain decoders before the user can.
		if _, added := kl.decoders[topic]; !added {
			kl.decoders[topic] = makeDecoder
		}
	}
}

func (kl *kafkalog) AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string) {
	for _, topic := range topics {
		// Only add the encoder if it has not been added before, this is
		// used to register certain encoders before the user can.
		if _, added := kl.encoders[topic]; !added {
			kl.encoders[topic] = makeEncoder
		}
	}
}

func (kl *kafkalog) EncodedTopics() map[string]bool {
	encoded := make(map[string]bool)
	for topic, _ := range kl.encoders {
		encoded[topic] = true
	}
	return encoded
}

func (kl *kafkalog) DecodedTopics() map[string]bool {
	decoded := make(map[string]bool)
	for topic, _ := range kl.decoders {
		decoded[topic] = true
	}
	return decoded
}

func (kl *kafkalog) Partitions(topic string) ([]int32, error) {
	parts, err := kl.client.Partitions(topic)
	if err != nil {
		return nil, err
	}
	return parts, err
}

func (kl *kafkalog) Offsets(topic string, part int32) (int64, int64, error) {
	min, err := kl.client.GetOffset(topic, part, sarama.OffsetOldest)
	if err != nil {
		return 0, 0, err
	}
	max, err := kl.client.GetOffset(topic, part, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, err
	}

	return min, max, nil
}

func (kl *kafkalog) AddPartitioner(p Partitioner, topics ...string) {
	for _, topic := range topics {
		if _, added := kl.partitioners[topic]; !added {
			kl.partitioners[topic] = p
		}
	}
}

func cloneConfig(conf *sarama.Config) *sarama.Config {
	conf2 := sarama.NewConfig()

	conf2.Net.MaxOpenRequests = conf.Net.MaxOpenRequests
	conf2.Net.DialTimeout = conf.Net.DialTimeout
	conf2.Net.ReadTimeout = conf.Net.ReadTimeout
	conf2.Net.WriteTimeout = conf.Net.WriteTimeout
	conf2.Net.KeepAlive = conf.Net.KeepAlive

	conf2.Metadata.Retry.Max = conf.Metadata.Retry.Max
	conf2.Metadata.Retry.Backoff = conf.Metadata.Retry.Backoff
	conf2.Metadata.RefreshFrequency = conf.Metadata.RefreshFrequency

	conf2.Producer.MaxMessageBytes = conf.Producer.MaxMessageBytes
	conf2.Producer.RequiredAcks = conf.Producer.RequiredAcks
	conf2.Producer.Timeout = conf.Producer.Timeout
	conf2.Producer.Compression = conf.Producer.Compression
	conf2.Producer.Partitioner = conf.Producer.Partitioner

	conf2.Producer.Return.Successes = conf.Producer.Return.Successes
	conf2.Producer.Return.Errors = conf.Producer.Return.Errors
	conf2.Producer.Flush.Bytes = conf.Producer.Flush.Bytes
	conf2.Producer.Flush.Messages = conf.Producer.Flush.Messages
	conf2.Producer.Flush.Frequency = conf.Producer.Flush.Frequency
	conf2.Producer.Flush.MaxMessages = conf.Producer.Flush.MaxMessages
	conf2.Producer.Retry.Max = conf.Producer.Retry.Max
	conf2.Producer.Retry.Backoff = conf.Producer.Retry.Backoff

	conf2.Consumer.Retry.Backoff = conf.Consumer.Retry.Backoff
	conf2.Consumer.Fetch.Min = conf.Consumer.Fetch.Min
	conf2.Consumer.Fetch.Default = conf.Consumer.Fetch.Default
	conf2.Consumer.Fetch.Max = conf.Consumer.Fetch.Max
	conf2.Consumer.MaxWaitTime = conf.Consumer.MaxWaitTime
	conf2.Consumer.Return.Errors = conf.Consumer.Return.Errors

	conf2.ClientID = conf.ClientID
	conf2.ChannelBufferSize = conf.ChannelBufferSize

	return conf2
}

func (kl *kafkalog) Write(topic string, in <-chan Event) {
	go func() {
		conf := cloneConfig(kl.conf.Config)
		conf.Producer.Partitioner = kl.newPartitioner(topic)
		if topic == kl.conf.cmdtopic {
			conf.Producer.Flush.Messages = 2
			conf.Producer.Flush.Frequency = 50 * time.Millisecond
		}

		client, err := sarama.NewClient(kl.conf.Brokers, conf)
		if err != nil {
			Seppuku("fatal: client: topic: %v: %v", topic, err)
		}
		defer client.Close()

		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			Seppuku("fatal: producer: topic: %v: %v", err)
		}
		defer producer.Close()

		var buf bytes.Buffer
		for event := range in {
			if event == nil {
				continue
			}
			buf.Reset()
			enc := kl.encoders[topic](&buf)
			err := enc.Encode(event.Message())
			if err != nil {
				log.Printf("error: producer: topic: %v: encode failed: %v", topic, err)
			} else {
				key := []byte(event.Key())
				val := make([]byte, buf.Len())
				buf.Read(val)
				select {
				case producer.Input() <- &sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.ByteEncoder(key),
					Value: sarama.ByteEncoder(val),
				}:
				case err := <-producer.Errors():
					if strings.Contains(err.Error(), "Message was too large") {
						log.Printf("error: producer: topic: %v: %T :: %v", topic, err.Err, err.Error)
					} else {
						Seppuku("fatal: producer: topic: %v: %T :: %v", topic, err.Err, err.Error)
					}
				}
			}
		}
	}()
}

func (kl *kafkalog) Read(topic string, parts []int32, offsets []int64, exit <-chan bool) <-chan Event {

	// Consumers read from the real topic and push data
	// into the out channel.
	out := make(chan Event, 0)

	// Setup a wait group so that the out channel
	// can be closed when all consumers have
	// exited.
	wg := new(sync.WaitGroup)

	client, err := sarama.NewClient(kl.conf.Brokers, kl.conf.Config)
	if err != nil {
		Seppuku("fatal: client: topic: %v: %v", topic, err)
	}

	for i, part := range parts {
		wg.Add(1)

		go func(wg *sync.WaitGroup, part int32, offset int64, out chan<- Event) {
			defer wg.Done()

			consumer, err := sarama.NewConsumerFromClient(client)
			if err != nil {
				Seppuku("fatal: consumer: topic: %v: %v", topic, err)
			}

			go func() {
				defer consumer.Close()
				defer client.Close()
				<-exit
			}()

			var buf bytes.Buffer
			events, err := consumer.ConsumePartition(topic, part, offset)
			if err != nil {
				Seppuku("fatal: consumer: topic: %v: %v", topic, err)
			}
			for {
				select {
				case err, ok := <-events.Errors():
					if !ok {
						return
					}
					log.Printf("error: consumer: topic: %v: partition: %v: %v", topic, part, err.Err)
				case event, ok := <-events.Messages():
					if !ok {
						return
					}
					buf.Reset()
					dec := kl.decoders[topic](&buf)
					buf.Write(event.Value)
					msg := dec.New()
					err = dec.Decode(msg)

					if err != nil {
						errmsg := fmt.Errorf("consumer: topic: %v: partition: %v: offset: %v: instance-type: %T: byte buffer length: %v: %v", topic, part, event.Offset, msg, len(buf.Bytes()), err)
						select {
						case out <- NewReadable(event.Topic, event.Partition, event.Offset, errmsg):
						case <-exit:
						}
					} else {
						select {
						case out <- NewReadable(event.Topic, event.Partition, event.Offset, msg):
						case <-exit:
						}
					}
				}
			}
		}(wg, part, offsets[i], out)
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

func (kl *kafkalog) newPartitioner(topic string) sarama.PartitionerConstructor {
	if p, found := kl.partitioners[topic]; found {
		return func(topic string) sarama.Partitioner { return &wrappartitioner{p: p} }
	} else {
		return func(topic string) sarama.Partitioner { return &partitioner{} }
	}
}

type wrappartitioner struct {
	p Partitioner
}

func (p *wrappartitioner) RequiresConsistency() bool {
	return true
}

func (w *wrappartitioner) Partition(msg *sarama.ProducerMessage, nparts int32) (int32, error) {
	bytes, err := msg.Key.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode key: %v", msg.Key)
	}
	return w.p.Partition(bytes, nparts), nil
}

type partitioner struct {
}

func (p *partitioner) RequiresConsistency() bool {
	return true
}

func (p *partitioner) Partition(msg *sarama.ProducerMessage, nparts int32) (int32, error) {
	bytes, err := msg.Key.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode key: %v: error: %v", msg.Key, err)
	}
	if len(bytes) == 0 {
		return 0, nil
	}
	hasher := fnv.New32a()
	_, err = hasher.Write(bytes)
	if err != nil {
		return 0, fmt.Errorf("failed to hash key: %v: error:", msg.Key, err)
	}
	hash := int32(hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return hash % nparts, nil
}
