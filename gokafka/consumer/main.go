package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	log.Println("starting consumer ... ")
	consumer := NewKafkaConsumer()

	topics := []string{"test"}
	err := consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Println(err)
	}

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Println(err)
		}

		log.Println(string(msg.Value), " ", msg.TopicPartition)
	}
}

func NewKafkaConsumer() *kafka.Consumer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "goapp_consumer",
		"group.id":          "goapp_group",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Println(err)
	}

	return c
}
