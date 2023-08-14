package data

import (
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

type RequestContext struct {
	ContextID string
}

type SetupContext struct {
	TransactionID string
	CallbackPath  string
}

type SetupResponse struct {
	TransactionID string
}

func GetKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}
