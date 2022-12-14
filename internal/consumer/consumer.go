package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Egor-Tihonov/Kafka-proj/internal/models"
	"github.com/Egor-Tihonov/Kafka-proj/internal/repository"
	"github.com/jackc/pgx/v4"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	Conn *kafka.Conn
}

func NewConsumer() (*Consumer, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "quickstart-events", 0)
	if err != nil {
		return nil, fmt.Errorf("producer: can't create new instance - %e", err)
	}
	return &Consumer{Conn: conn}, nil
}

func (c Consumer) ReadMessages(rps *repository.PostgresR) error {
	pgxBatch := pgx.Batch{}
	messages := c.Conn.ReadBatch(10e1, 10e5)
	bytes := make([]byte, 10e5)
	for {
		n, err := messages.Read(bytes)
		if err != nil {
			break
		}
		message := models.Message{}
		messagesString := string(bytes[:n])

		err = json.Unmarshal([]byte(messagesString), &message)
		if err != nil {
			log.Printf("internal/consumer: unmarshal error, %e", err)
			return err
		}
		pgxBatch.Queue("insert into tablekafka(message) values($1)", message.NewMessage)

	}
	err := rps.AddToDB(context.Background(), &pgxBatch)
	if err != nil {
		return err
	}
	err = messages.Close()
	if err != nil {
		return fmt.Errorf("consumer: error while closing batch - %e", err)
	}
	return nil
}
