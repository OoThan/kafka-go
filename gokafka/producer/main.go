package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

func main() {
	log.Println("starting ... ")
	producer := NewKafkaProducer()
	defer producer.Flush(10)

	deliveryChan := make(chan kafka.Event)
	go DeliveryReport(deliveryChan)
	i := 0
	for {
		Publish("Hello World "+fmt.Sprint(i), "test", producer, nil, deliveryChan)
		time.Sleep(5 * time.Second)
		i++
	}
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "localhost:9092",
		"delivery.timeout.ms": "1",
		"acks":                "all",
		"enable.idempotence":  "true",
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err)
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch e.(type) {
		case *kafka.Message:
			e := <-deliveryChan
			msg := e.(*kafka.Message)

			if msg.TopicPartition.Error != nil {
				log.Println("error in topic partition")
			} else {
				log.Println("Message event : ", msg.TopicPartition)
				// note in the database that the message was processed.
				// ex: confirm that a bank transfer has taken place
			}
		}

	}
}
