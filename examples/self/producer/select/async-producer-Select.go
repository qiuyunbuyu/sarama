package main

import (
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
)

//This example shows how to use the producer with separate goroutines reading from the Successes and Errors channels.
//Note that in order for the Successes channel to be populated, you have to set config.Producer.Return.Successes to true.

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"kafka9001:9092"}, config)
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

	var enqueued, producerErrors, successes int

ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "EOC_ALERT_MESSAGE_TOPIC", Value: sarama.StringEncoder("testing 123")}:
			enqueued++
		case message := <-producer.Successes():
			successes++
			log.Println("---", message)
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}
