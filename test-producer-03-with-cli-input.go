package main

/*
	Refactored for brevity.
	Extract constants.
	Command-line input overrides.
*/

import (
	"flag"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

var KAFKA_URL = "localhost:9092"
var DEFAULT_MSG = "sample msg 3"
var DEFAULT_TOPIC = "test"
var WAIT_TIME = 2

func main() {
	// get command-line inputs
	msgPtr := flag.String("msg", DEFAULT_MSG, "message to send")
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

	// create, send msg to the producer's Input channel
	producer.Input() <- &sarama.ProducerMessage{
		Topic: *topicPtr,
		Value: sarama.StringEncoder(*msgPtr),
	}

	// monitor producer's feedback channels
	select {
	case err := <-producer.Errors():
		log.Fatalln("FAILED TO SEND MSG")
		panic(err.Err)
	case _ = <-time.After(time.Second * time.Duration(WAIT_TIME)):
		log.Println("SUCCESS")
	}
}
