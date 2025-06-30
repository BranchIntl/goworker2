package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BranchIntl/goworker2/brokers/redis"
	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/registry"
	"github.com/BranchIntl/goworker2/serializers/resque"
	resqueStats "github.com/BranchIntl/goworker2/statistics/resque"
)

func myFunc(queue string, args ...interface{}) error {
	fmt.Printf("From %s, %v\n", queue, args)
	// Simulate some work
	time.Sleep(500 * time.Millisecond)
	return nil
}

func main() {
	// This example shows how to use core components directly for maximum control
	// For simpler setup, see redis-basic example using pre-configured engines

	// Create Redis broker with custom options
	brokerOpts := redis.DefaultOptions()
	brokerOpts.URI = "redis://localhost:6379/"
	brokerOpts.Namespace = "resque:"
	brokerOpts.Queues = []string{"critical", "default", "low"}
	brokerOpts.PollInterval = 2 * time.Second
	brokerOpts.MaxConnections = 20
	brokerOpts.MaxIdle = 5
	brokerOpts.ConnectTimeout = 15 * time.Second

	// Create serializer with custom settings
	serializer := resque.NewSerializer()
	serializer.SetUseNumber(true)

	// Create broker
	broker := redis.NewBroker(brokerOpts, serializer)

	// Create Resque statistics with custom options
	statsOpts := resqueStats.DefaultOptions()
	statsOpts.URI = "redis://localhost:6379/"
	statsOpts.Namespace = "resque:"
	statsOpts.MaxConnections = 5
	stats := resqueStats.NewStatistics(statsOpts)

	// Create registry
	reg := registry.NewRegistry()

	// Create engine with detailed configuration
	engine := core.NewEngine(
		broker,
		stats,
		reg,
		core.WithConcurrency(10),
		core.WithShutdownTimeout(30*time.Second),
		core.WithJobBufferSize(200),
	)

	// Register job handlers
	reg.Register("MyClass", myFunc)

	// Start the engine manually for full control
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		log.Fatal("Error starting engine:", err)
	}

	// Custom signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGUSR1)

	fmt.Println("Worker started. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	for sig := range sigChan {
		switch sig {
		case syscall.SIGUSR1:
			// Custom behavior for SIGUSR1 - print health status
			health := engine.Health()
			fmt.Printf("Health Status: Healthy=%v, Active Workers=%d\n",
				health.Healthy, health.ActiveWorkers)
			for queue, count := range health.QueuedJobs {
				fmt.Printf("  Queue %s: %d jobs\n", queue, count)
			}
		default:
			fmt.Printf("Received signal %v, shutting down...\n", sig)
			goto shutdown
		}
	}

shutdown:
	// Graceful shutdown
	if err := engine.Stop(); err != nil {
		log.Printf("Error stopping engine: %v", err)
	}
	fmt.Println("Worker stopped.")
}
