package main

/*
	uses sarama-cluster package
*/

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	cluster "github.com/bsm/sarama-cluster"
)

var KAFKA_URL = "127.0.0.1:9092"
var DEFAULT_TOPIC = "test"
var DEFAULT_CONSUMER_GROUP = "my-consumer-group"

func main() {
	topic := flag.String("topic", DEFAULT_TOPIC, "topic to subscribe to")
	consumerGroup := flag.String("group", DEFAULT_CONSUMER_GROUP, "consumer group")
	flag.Parse()

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// init consumer
	brokers := []string{KAFKA_URL}
	topics := []string{*topic} // can do multiple := []string{"my_topic", "other_topic"}
	consumer, err := cluster.NewConsumer(brokers, *consumerGroup, topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case e := <-consumer.Errors():
			log.Printf("Error: %s\n", e.Error())
		case ntf := <-consumer.Notifications():
			log.Printf("Rebalanced: %+v\n", ntf)
		case <-signals:
			return
		}
	}
}
