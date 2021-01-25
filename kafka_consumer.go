package quacktorstreams

import (
	"github.com/Azer0s/quacktors/quacktorstreams"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type KafkaConsumer struct {
	Config *kafka.ConfigMap
	c      *kafka.Consumer
}

func (k *KafkaConsumer) Init() error {
	var err error
	k.c, err = kafka.NewConsumer(k.Config)

	if err != nil {
		return err
	}

	return nil
}

func (k *KafkaConsumer) Subscribe(topic string) error {
	err := k.c.Subscribe(topic, nil)

	if err != nil {
		return err
	}

	return nil
}

func (k *KafkaConsumer) NextMessage() (quacktorstreams.StreamMessage, error) {
	msg, err := k.c.ReadMessage(-1)

	if err != nil {
		return quacktorstreams.StreamMessage{}, err
	}

	return quacktorstreams.StreamMessage{
		Bytes: msg.Value,
		Topic: *msg.TopicPartition.Topic,
		Meta:  msg,
	}, nil
}
