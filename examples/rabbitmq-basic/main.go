package main

import (
	"fmt"
	"log"

	"github.com/benmanns/goworker"
)

func myFunc(queue string, args ...interface{}) error {
	fmt.Printf("From %s, %v\n", queue, args)
	return nil
}

func init() {
	settings := goworker.WorkerSettings{
		BrokerType:     "rabbitmq",
		RabbitMQURI:    "amqp://guest:guest@localhost:5672/",
		Connections:    100,
		Queues:         []string{"goworker_queue"},
		QueuesString:   "goworker_queue",
		UseNumber:      true,
		ExitOnComplete: false,
		Concurrency:    2,
		IntervalFloat: 5.0,
		Exchange:      "activejob",
		ExchangeType:  "direct",
		PrefetchCount: 1,
	}
	goworker.SetSettings(settings)
	goworker.Register("JobSystemTestJob", myFunc)
}

func main() {
	if err := goworker.Work(); err != nil {
		log.Fatal("Error:", err)
	}
}
