package main

/*
	uses sarama package
*/

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

const KAFKA_URL string = "localhost:9092"
const DEFAULT_TOPIC string = "test"

func main() {
	// parse input for topic to subscribe to
	topicPtr := flag.String("topic", DEFAULT_TOPIC, "kafka topic to subscribe to")
	flag.Parse()

	log.Printf("STARTED LISTENING ON URL=%s FOR TOPIC=%s\n", KAFKA_URL, *topicPtr)

	// create consumer, which keeps a connection to the kafka server
	consumer, err := sarama.NewConsumer([]string{KAFKA_URL}, nil)
	if err != nil {
		panic(err)
	}

	// remember to close connection at end
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// subscribe to topic
	// partitionConsumer, err := consumer.ConsumePartition(*topicPtr, 0, sarama.OffsetNewest) // sarama.OffsetOldest
	partitionConsumer, err := consumer.ConsumePartition(*topicPtr, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	// log.Println("OffsetNewest: %d, OffsetOldest: %d", sarama.OffsetNewest, sarama.OffsetOldest)

	// remember to close connection at end
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	sigintChannel := make(chan os.Signal, 1)
	signal.Notify(sigintChannel, os.Interrupt)

	// Start listening
	consumedCtr := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("CONSUMED MSG: %s %+v\n", msg.Value, msg)
			consumedCtr++
		case <-sigintChannel:
			break ConsumerLoop
		}
	}

	log.Printf("TERMINATING. Consumed: %d\n", consumedCtr)
}
