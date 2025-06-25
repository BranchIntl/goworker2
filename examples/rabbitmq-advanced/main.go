package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BranchIntl/goworker2/brokers/rabbitmq"
	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/registry"
	"github.com/BranchIntl/goworker2/serializers/sneakers"
	"github.com/BranchIntl/goworker2/statistics/resque"
)

func myFunc(queue string, args ...interface{}) error {
	fmt.Printf("From %s, %v\n", queue, args)
	// Simulate some work
	time.Sleep(1 * time.Second)
	return nil
}

func imageProcessingFunc(queue string, args ...interface{}) error {
	fmt.Printf("Processing image from %s: %v\n", queue, args)
	// Simulate heavy work
	time.Sleep(3 * time.Second)
	return nil
}

func main() {
	// This example shows how to use core components directly for maximum control
	// For simpler setup, see rabbitmq-basic example using pre-configured engines

	// Create RabbitMQ broker with custom options
	brokerOpts := rabbitmq.DefaultOptions()
	brokerOpts.URI = "amqp://guest:guest@localhost:5672/"
	brokerOpts.Exchange = "activejob"
	brokerOpts.PrefetchCount = 2

	// Create serializer
	serializer := sneakers.NewSerializer()

	// Create broker
	broker := rabbitmq.NewBroker(brokerOpts, serializer)

	// Use Redis for statistics instead of NoOp for this advanced example
	statsOpts := resque.DefaultOptions()
	statsOpts.URI = "redis://localhost:6379/"
	statsOpts.Namespace = "goworker:stats:"
	stats := resque.NewStatistics(statsOpts)

	// Create registry
	reg := registry.NewRegistry()

	// Create engine with detailed configuration
	engine := core.NewEngine(
		broker,
		stats,
		reg,
		serializer,
		core.WithConcurrency(5),
		core.WithQueues([]string{"activejob", "images", "critical"}),
		core.WithPollInterval(3*time.Second),
		core.WithShutdownTimeout(45*time.Second),
		core.WithJobBufferSize(50),
		core.WithExitOnComplete(false),
		core.WithStrictQueues(false), // Random queue order
	)

	// Register multiple job handlers
	reg.Register("MyClass", myFunc)
	reg.Register("ImageProcessingJob", imageProcessingFunc)

	// Create some queues with specific options
	ctx := context.Background()
	queueOpts := core.QueueOptions{
		MessageTTL:      30 * time.Minute,
		DeadLetterQueue: "failed",
		MaxRetries:      3,
	}

	if err := broker.Connect(ctx); err != nil {
		log.Fatal("Failed to connect broker:", err)
	}

	broker.CreateQueue(ctx, "activejob", queueOpts)
	broker.CreateQueue(ctx, "images", queueOpts)
	broker.CreateQueue(ctx, "critical", core.QueueOptions{
		MessageTTL: 10 * time.Minute,
		MaxRetries: 5,
	})

	// Start the engine manually for full control
	if err := engine.Start(ctx); err != nil {
		log.Fatal("Error starting engine:", err)
	}

	// Custom signal handling with health monitoring
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2)

	fmt.Println("Worker started. Press Ctrl+C to stop, SIGUSR1 for health, SIGUSR2 for queue stats.")

	// Wait for shutdown signal
	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGUSR1:
				// Print health status
				health := engine.Health()
				fmt.Printf("\n=== Health Status ===\n")
				fmt.Printf("Healthy: %v\n", health.Healthy)
				fmt.Printf("Active Workers: %d\n", health.ActiveWorkers)
				if health.BrokerHealth != nil {
					fmt.Printf("Broker Error: %v\n", health.BrokerHealth)
				}
				if health.StatsHealth != nil {
					fmt.Printf("Stats Error: %v\n", health.StatsHealth)
				}
				fmt.Printf("=====================\n\n")

			case syscall.SIGUSR2:
				// Print queue statistics
				fmt.Printf("\n=== Queue Statistics ===\n")
				health := engine.Health()
				for queue, count := range health.QueuedJobs {
					fmt.Printf("Queue %s: %d jobs\n", queue, count)
				}
				fmt.Printf("========================\n\n")

			default:
				fmt.Printf("Received signal %v, shutting down...\n", sig)
				goto shutdown
			}
		}
	}

shutdown:
	// Graceful shutdown
	fmt.Println("Starting graceful shutdown...")
	if err := engine.Stop(); err != nil {
		log.Printf("Error stopping engine: %v", err)
	}
	fmt.Println("Worker stopped.")
}
