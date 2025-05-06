package main

import (
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
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

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                                  sync.WaitGroup
		enqueued, successes, producerErrors int
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			producerErrors++
		}
	}()

	//ProducerLoop: 定义了一个名为 ProducerLoop 的标签
ProducerLoop:
	for {
		message := &sarama.ProducerMessage{Topic: "EOC_ALERT_MESSAGE_TOPIC", Value: sarama.StringEncoder("testing 123")}
		select {
		case producer.Input() <- message:
			enqueued++

		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
		}
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, producerErrors)
}
