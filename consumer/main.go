package main

import (
	"log"

	"github.com/Egor-Tihonov/Kafka-proj/internal/consumer"
	"github.com/Egor-Tihonov/Kafka-proj/internal/repository"
)

func main() {
	cons, err := consumer.NewConsumer()
	defer func() {
		err := cons.Conn.Close()
		if err != nil {
			log.Fatalf("error %e", err)
		}

	}()
	if err != nil {
		log.Fatalf("error %e", err)
	}
	log.Println("successfully create consumer...")
	rps, err := repository.NewConnection()
	if err != nil {
		log.Fatalf("error %e", err)
	}
	err = cons.ReadMessages(rps)
	if err != nil {
		log.Fatalf("error %e", err)
	}
	log.Println("successfully add to db")
}
