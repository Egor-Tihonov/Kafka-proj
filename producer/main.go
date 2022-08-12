package main

import (
	"log"

	"github.com/Egor-Tihonov/Kafka-proj/internal/producer"
)

const count = 2000

func main() {
	prod, err := producer.NewProducer()
	if err != nil {
		log.Fatal("cannot create new producer", err)
	}
	defer func() {
		err := prod.Conn.Close()
		if err != nil {
			log.Fatal("cannot close connection with kafka topic", err)
		}
	}()
	log.Println("producer successfully created...")
	err = prod.WriteMessagesToTopic(count)
	if err != nil {
		log.Fatal("error with write messages", err)
	}
	log.Print("all messages successfully send")
}
