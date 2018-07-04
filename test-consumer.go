package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"flag"
)

const KAFKA_URL string = "localhost:9092"
const DEFAULT_TOPIC string = "test"

func main() {
    // parse input flags
    topicPtr := flag.String("topic", DEFAULT_TOPIC, "kafka topic to subscribe to")
    flag.Parse()
    
	log.Printf("STARTED LISTENING ON URL=%s FOR TOPIC=%s\n", KAFKA_URL, *topicPtr)
	
	consumer, err := sarama.NewConsumer([]string{KAFKA_URL}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(*topicPtr, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumedCtr := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("CONSUMED MSG: %s\n", msg.Value)
			consumedCtr++
		case <-signals:
			break ConsumerLoop
		}
	}

	log.Printf("TERMINATING. Consumed: %d\n", consumedCtr)
}
