package quacktorstreams

import (
	"encoding/json"
	"github.com/Azer0s/quacktors"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type KafkaProducer struct {
	Config *kafka.ConfigMap
	p      *kafka.Producer
	topic  string
}

func (k *KafkaProducer) Init() error {
	var err error
	k.p, err = kafka.NewProducer(k.Config)

	if err != nil {
		return err
	}

	return nil
}

func (k *KafkaProducer) SetTopic(topic string) {
	k.topic = topic
}

func (k *KafkaProducer) Emit(message quacktors.Message) {
	val, err := json.Marshal(message)

	if err != nil {
		return
	}

	_ = k.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
		Value:          val,
	}, nil)
}
