package grid

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type ReadWriteLog interface {
	Write(topic string, in <-chan Event)
	Read(topic string, parts []int32) <-chan Event
	AddEncoder(makeEncoder func(io.Writer) Encoder, topics ...string)
	AddDecoder(makeDecoder func(io.Reader) Decoder, topics ...string)
	AddPartitioner(p func(key io.Reader, parts int32) int32, topics ...string)
	EncodedTopics() map[string]bool
	DecodedTopics() map[string]bool
	Partitions(topic string) ([]int32, error)
}

type KafkaConfig struct {
	Brokers        []string
	BaseName       string
	ClientConfig   *sarama.ClientConfig
	ProducerConfig *sarama.ProducerConfig
	ConsumerConfig *sarama.ConsumerConfig
	cmdTopic       string
}

type kafkalog struct {
	conf         *KafkaConfig
	client       *sarama.Client
	encoders     map[string]func(io.Writer) Encoder
	decoders     map[string]func(io.Reader) Decoder
	partitioners map[string]func(io.Reader, int32) int32
}

func NewKafkaReadWriteLog(id string, conf *KafkaConfig) (ReadWriteLog, error) {
	client, err := sarama.NewClient(id, conf.Brokers, conf.ClientConfig)
	if err != nil {
		return nil, err
	}

	return &kafkalog{conf: conf, client: client, encoders: make(map[string]func(io.Writer) Encoder), decoders: make(map[string]func(io.Reader) Decoder)}, nil
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

func (kl *kafkalog) AddPartitioner(p func(key io.Reader, parts int32) int32, topics ...string) {
	for _, topic := range topics {
		if _, added := kl.partitioners[topic]; !added {
			kl.partitioners[topic] = p
		}
	}
}

func cloneProducerConfig(conf *sarama.ProducerConfig) *sarama.ProducerConfig {
	return &sarama.ProducerConfig{
		RequiredAcks:      conf.RequiredAcks,      // The level of acknowledgment reliability needed from the broker (defaults to WaitForLocal).
		Timeout:           conf.Timeout,           // The maximum duration the broker will wait the receipt of the number of RequiredAcks. This is only relevant when RequiredAcks is set to WaitForAll or a number > 1. Only supports millisecond resolution, nanoseconds will be truncated.
		Compression:       conf.Compression,       // The type of compression to use on messages (defaults to no compression).
		FlushMsgCount:     conf.FlushMsgCount,     // The number of messages needed to trigger a flush.
		FlushFrequency:    conf.FlushFrequency,    // If this amount of time elapses without a flush, one will be queued.
		FlushByteCount:    conf.FlushByteCount,    // If this many bytes of messages are accumulated, a flush will be triggered.
		AckSuccesses:      conf.AckSuccesses,      // If enabled, successfully delivered messages will be returned on the Successes channel.
		MaxMessageBytes:   conf.MaxMessageBytes,   // The maximum permitted size of a message (defaults to 1000000)
		ChannelBufferSize: conf.ChannelBufferSize, // The size of the buffers of the channels between the different goroutines. Defaults to 0 (unbuffered).
	}
}

func (kl *kafkalog) Write(topic string, in <-chan Event) {
	go func() {
		name := fmt.Sprintf("grid_writer_%s_topic_%s", kl.conf.BaseName, topic)
		client, err := sarama.NewClient(name, kl.conf.Brokers, kl.conf.ClientConfig)
		if err != nil {
			log.Fatalf("fatal: topic: %v: client: %v", topic, err)
		}
		defer client.Close()
		conf := cloneProducerConfig(kl.conf.ProducerConfig)
		conf.Partitioner = kl.newPartitioner(topic)
		if topic == kl.conf.cmdTopic {
			conf.FlushMsgCount = 2
			conf.FlushFrequency = 50 * time.Millisecond
		}

		producer, err := sarama.NewProducer(client, conf)
		if err != nil {
			log.Fatalf("fatal: topic: %v: producer: %v", err)
		}
		defer producer.Close()

		var buf bytes.Buffer
		for event := range in {
			buf.Reset()
			enc := kl.encoders[topic](&buf)
			err := enc.Encode(event.Message())
			if err != nil {
				log.Printf("error: topic: %v: encode failed: %v", topic, err)
			} else {
				key := []byte(event.Key())
				val := make([]byte, buf.Len())
				buf.Read(val)
				select {
				case producer.Input() <- &sarama.MessageToSend{
					Topic: topic,
					Key:   sarama.ByteEncoder(key),
					Value: sarama.ByteEncoder(val),
				}:
				case err := <-producer.Errors():
					log.Fatalf("fatal: topic: %v: producer: %v", err.Err)
				}
			}
		}
	}()
}

func (kl *kafkalog) Read(topic string, parts []int32) <-chan Event {

	// Consumers read from the real topic and push data
	// into the out channel.
	out := make(chan Event, 0)

	// Setup a wait group so that the out channel
	// can be closed when all consumers have
	// exited.
	wg := new(sync.WaitGroup)

	for _, part := range parts {
		wg.Add(1)

		go func(wg *sync.WaitGroup, part int32, out chan<- Event) {
			defer wg.Done()

			name := fmt.Sprintf("grid_reader_%s_topic_%s_part_%d", kl.conf.BaseName, topic, part)
			client, err := sarama.NewClient(name, kl.conf.Brokers, kl.conf.ClientConfig)
			if err != nil {
				log.Fatalf("fatal: topic: %v: client: %v", topic, err)
			}
			defer client.Close()

			config := sarama.NewConsumerConfig()
			config.OffsetMethod = sarama.OffsetMethodNewest

			consumer, err := sarama.NewConsumer(client, topic, part, name, config)
			if err != nil {
				log.Fatalf("fatal: topic: %v: consumer: %v", topic, err)
			}
			defer consumer.Close()

			var buf bytes.Buffer
			for event := range consumer.Events() {
				buf.Reset()
				dec := kl.decoders[topic](&buf)
				buf.Write(event.Value)
				msg := dec.New()
				err = dec.Decode(msg)

				if err != nil {
					log.Printf("error: topic: %v decode failed: %v: msg: %v value: %v", topic, err, msg, string(buf.Bytes()))
				} else {
					out <- NewReadable(event.Topic, event.Offset, msg)
				}
			}
		}(wg, part, out)
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

func (kl *kafkalog) newPartitioner(topic string) func() sarama.Partitioner {
	if p, found := kl.partitioners[topic]; found {
		return func() sarama.Partitioner { return &wrappartitioner{p: p, buf: new(bytes.Buffer)} }
	} else {
		return func() sarama.Partitioner { return &partitioner{hasher: fnv.New32a()} }
	}
}

type wrappartitioner struct {
	buf *bytes.Buffer
	p   func(key io.Reader, parts int32) int32
}

func (w *wrappartitioner) Partition(key sarama.Encoder, numPartitions int32) int32 {
	bytes, err := key.Encode()
	if err != nil {
		return 0
	}
	w.buf.Write(bytes)
	return w.p(w.buf, numPartitions)
}

type partitioner struct {
	hasher hash.Hash32
}

func (p *partitioner) Partition(key sarama.Encoder, numPartitions int32) int32 {
	bytes, err := key.Encode()
	if err != nil {
		return 0
	}
	if len(bytes) == 0 {
		return 0
	}
	p.hasher.Reset()
	_, err = p.hasher.Write(bytes)
	if err != nil {
		return 0
	}
	hash := int32(p.hasher.Sum32())
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions
}
