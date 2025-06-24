package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/benmanns/goworker/brokers/rabbitmq"
	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/registry"
	"github.com/benmanns/goworker/serializers/sneakers"
	rabbitmqStats "github.com/benmanns/goworker/statistics/rabbitmq"
)

func myFunc(queue string, args ...interface{}) error {
	fmt.Printf("From %s, %v\n", queue, args)
	return nil
}

func main() {
	// Create RabbitMQ broker with its own options
	brokerOpts := rabbitmq.DefaultOptions()
	brokerOpts.URI = "amqp://guest:guest@localhost:5672/"
	brokerOpts.Exchange = "goworker"
	brokerOpts.PrefetchCount = 1

	// Create serializer
	serializer := sneakers.NewSerializer()

	// Create broker
	broker := rabbitmq.NewBroker(brokerOpts, serializer)

	// Create RabbitMQ statistics with its own options
	statsOpts := rabbitmqStats.DefaultOptions()
	statsOpts.URI = "amqp://guest:guest@localhost:5672/"
	stats := rabbitmqStats.NewStatistics(statsOpts)

	// Create registry
	reg := registry.NewRegistry()

	// Create engine with engine-specific options
	engine := core.NewEngine(
		broker,
		stats,
		reg,
		serializer,
		core.WithConcurrency(2),
		core.WithQueues([]string{"myqueue"}),
		core.WithPollInterval(5*time.Second),
		core.WithExitOnComplete(false),
	)

	// Register job handler
	reg.Register("MyClass", myFunc)

	// Start the engine and wait for shutdown signals
	// This is the simple approach using the convenience Run() method
	ctx := context.Background()
	if err := engine.Run(ctx); err != nil {
		log.Fatal("Error:", err)
	}

	// Alternative approach for more control:
	// if err := engine.Start(ctx); err != nil {
	//     log.Fatal("Error:", err)
	// }
	//
	// // Set up signal handling
	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	//
	// // Wait for shutdown signal
	// sig := <-sigChan
	// fmt.Printf("Received signal %v, shutting down...\n", sig)
	//
	// // Stop the engine
	// if err := engine.Stop(); err != nil {
	//     log.Printf("Error stopping engine: %v", err)
	// }
}
