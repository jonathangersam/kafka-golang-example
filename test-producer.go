package main

import (
	// "flag"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
)

const KAFKA_URL string = "localhost:9092"
const DEFAULT_TOPIC string = "test"

func main() {
	// crete producer object
	producer, err := sarama.NewAsyncProducer([]string{KAFKA_URL}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueuedCtr, errors int

	/* FIXME: this is DDOS! */
ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: DEFAULT_TOPIC, Key: nil, Value: sarama.StringEncoder("testing 123")}:
			enqueuedCtr++
		case err := <-producer.Errors():
			log.Println("FAILED to produce message", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("DONE. Enqueued: %d; errors: %d\n", enqueuedCtr, errors)
}
