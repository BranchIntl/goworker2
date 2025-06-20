package main

import (
	"fmt"
	"log"

	"github.com/benmanns/goworker"
)

func myFunc(queue string, args ...interface{}) error {
	if len(args) == 0 {
		log.Printf("Job received with no arguments")
        return nil
    }

	argMap, ok := args[0].(map[string]interface{})
    if !ok {
        return fmt.Errorf("expected map[string]interface{}, got %T", args[0])
    }

    delete(argMap, "_aj_ruby2_keywords")

    log.Printf("Job received on queue %s with arguments: %v", queue, argMap)

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
