package main

/*
	Basic implementation. Uses hardcoded values
*/

import (
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	// create connection
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln("FAILED TO CONNECT")
		panic(err)
	}

	// remember to close when done
	defer func() {
		err := producer.Close()
		if err != nil {
			panic(err)
		}
	}()

	// create msg struct
	msg := &sarama.ProducerMessage{
		Topic: "test",
		Value: sarama.StringEncoder("sample msg 1"),
	}

	// send msg to the producer's Input channel
	producer.Input() <- msg

	// set timeout for waiting
	done := make(chan bool)
	go func() {
		time.Sleep(time.Second * 2)
		done <- true
	}()

	// monitor producer's feedback channels
	select {
	case err := <-producer.Errors():
		log.Fatalln("FAILED TO SEND MSG")
		panic(err.Err)
	case _ = <-producer.Successes():
		log.Println("SUCCESS")
	case _ = <-done:
		log.Println("SUCCESS")
	}
}
