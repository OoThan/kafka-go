package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type Writer struct {
	Writer *kafka.Writer
}

func NewKafkaWriter() *Writer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "kafka_test1",
	}

	return &Writer{
		Writer: writer,
	}
}

func (k *Writer) WriteMessage(ctx context.Context, messages chan kafka.Message, messageCommit chan kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-messages:
			err := k.Writer.WriteMessages(ctx, kafka.Message{Value: m.Value})
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
			case messageCommit <- m:
			}
		}
	}
}
