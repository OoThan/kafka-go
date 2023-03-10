package kafka

import (
	"context"
	kafkago "github.com/segmentio/kafka-go"
)

type Writer struct {
	Writer *kafkago.Writer
}

func NewKafkaWriter() *Writer {
	writer := &kafkago.Writer{
		Addr:  kafkago.TCP("localhost:9092"),
		Topic: "kafka_test1",
	}

	return &Writer{
		Writer: writer,
	}
}

func (k *Writer) WriteMessage(ctx context.Context, messages chan kafkago.Message, messageCommit chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-messages:
			err := k.Writer.WriteMessages(ctx, kafkago.Message{Value: m.Value})
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
