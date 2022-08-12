package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Egor-Tihonov/Kafka-proj/internal/models"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	Conn *kafka.Conn
}

func NewProducer() (*Producer, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)
	if err != nil {
		return nil, fmt.Errorf("producer: can't create new instance - %e", err)
	}
	return &Producer{Conn: conn}, nil
}

func (p *Producer) WriteMessagesToTopic(countMessage int) error {
	var messages []kafka.Message
	for i := 1; i < countMessage; i++ {
		message := models.Message{NewMessage: "new massage"}
		mess, err := json.Marshal(message)
		if err != nil {
			break
		}
		messages = append(messages, kafka.Message{Value: mess})
	}
	for _, a := range messages {
		_, err := p.Conn.WriteMessages(a)
		if err != nil {
			return fmt.Errorf("producer couldnt send messages, %v", err)
		}
	}
	return nil
}
