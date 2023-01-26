package kafka

import "github.com/segmentio/kafka-go"

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
