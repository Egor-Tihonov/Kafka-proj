package main

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func main() {
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "topic_test", 0)
	messages := conn.ReadBatch(0, 1e3)
	bytes := make([]byte, 1e3)
	for {
		_, err := messages.Read(bytes)
		if err != nil {
			break
		}
		fmt.Println(string(bytes))
	}
}
