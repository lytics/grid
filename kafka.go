package grid

import (
	"bytes"
	"io"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

func StartTopicWriter(topic string, client *sarama.Client, newenc func(io.Writer) Encoder, in <-chan Event) {
	go func() {
		producer, err := sarama.NewSimpleProducer(client, topic, sarama.NewHashPartitioner)
		if err != nil {
			log.Fatalf("error: topic: failed to create producer: %v", err)
		}
		defer producer.Close()

		var buf bytes.Buffer
		enc := newenc(&buf)

		for event := range in {
			err := enc.Encode(event.Message())
			if err != nil {
				buf.Reset()
			} else {
				key := []byte(event.Key())
				val := make([]byte, buf.Len())
				buf.Read(val)
				producer.SendMessage(sarama.ByteEncoder(key), sarama.ByteEncoder(val))
			}
		}
	}()
}

func StartTopicReader(topic string, client *sarama.Client, newdec func(io.Reader) Decoder) <-chan Event {

	// Consumers read from the real topic and push data
	// into the out channel.
	out := make(chan Event, 0)

	// Setup a wait group so that the out channel
	// can be closed when all consumers have
	// exited.
	wg := new(sync.WaitGroup)

	partitions, err := client.Partitions(topic)
	if err != nil {
		log.Fatalf("error: topic: %v: failed getting kafka partition data: %v", topic, err)
	}

	for _, part := range partitions {
		wg.Add(1)

		go func(wg *sync.WaitGroup, part int32, out chan<- Event) {
			defer wg.Done()

			config := sarama.NewConsumerConfig()
			config.OffsetMethod = sarama.OffsetMethodNewest

			consumer, err := sarama.NewConsumer(client, topic, part, "kdjfkdjfkd", config)
			if err != nil {
				log.Fatalf("error: topic: %v consumer: %v", topic, err)
			}

			var buf bytes.Buffer
			dec := newdec(&buf)

			for e := range consumer.Events() {
				msg := dec.New()
				buf.Write(e.Value)
				err = dec.Decode(msg)
				if err != nil {
					log.Printf("error: topic: %v decode failed: %v: value: %v", topic, err, buf.Bytes())
					buf.Reset()
				} else {
					out <- NewReadable(e.Topic, e.Offset, msg)
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
