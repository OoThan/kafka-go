package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

type Reader struct {
	Reader *kafka.Reader
}

func NewKafkaReader() *Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "kafka_test",
		GroupID: "kafka_group",
	})

	return &Reader{
		Reader: reader,
	}
}

func (k *Reader) FetchMessage(ctx context.Context, messages chan<- kafka.Message) error {
	for {
		message, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
		case messages <- message:
			log.Printf("message fetched and sent to a channel: %v\n", string(message.Value))
		}
	}
}
