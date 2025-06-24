package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/benmanns/goworker/brokers/redis"
	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/registry"
	"github.com/benmanns/goworker/serializers/resque"
	resqueStats "github.com/benmanns/goworker/statistics/resque"
)

func myFunc(queue string, args ...interface{}) error {
	fmt.Printf("From %s, %v\n", queue, args)
	return nil
}

func main() {
	// Create Redis broker with its own options
	brokerOpts := redis.DefaultOptions()
	brokerOpts.URI = "redis://localhost:6379/"
	brokerOpts.Namespace = "resque:"

	// Create serializer
	serializer := resque.NewSerializer()

	// Create broker
	broker := redis.NewBroker(brokerOpts, serializer)

	// Create Resque statistics with its own options
	statsOpts := resqueStats.DefaultOptions()
	statsOpts.URI = "redis://localhost:6379/"
	statsOpts.Namespace = "resque:"
	stats := resqueStats.NewStatistics(statsOpts)

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
