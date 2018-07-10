package main

/*
	Refactored for brevity.
	Extract constants.
	Command-line input overrides.
	Interactive terminal input.
*/

import (
	"bufio"
	"flag"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

var KAFKA_URL = "localhost:9092"
var DEFAULT_TOPIC = "test"

func main() {
	// get command-line inputs
	topicPtr := flag.String("topic", DEFAULT_TOPIC, "topic to publish to")
	flag.Parse()

	// create connection
	producer, err := sarama.NewAsyncProducer([]string{KAFKA_URL}, nil)
	if err != nil {
		log.Fatalln("FAILED TO CONNECT")
		panic(err)
	}

	// remember to close when done
	defer func() {
		log.Println("Shutting Down")
		producer.Close()
	}()

	// monitor producer's feedback channels
	go func() {
		select {
		case err := <-producer.Errors():
			log.Fatalln("FAILED TO SEND MSG")
			panic(err.Err)
		}
	}()

	// loop to get user input
	scanner := bufio.NewScanner(os.Stdin)
	log.Println("Enter input msg/s. Type 'quit' to end:")
	for scanner.Scan() {
		terminal_input := scanner.Text()

		if terminal_input == "quit" {
			break
		}

		// create, send msg to the producer's Input channel
		producer.Input() <- &sarama.ProducerMessage{
			Topic: *topicPtr,
			Value: sarama.StringEncoder(terminal_input),
		}
	}
}
