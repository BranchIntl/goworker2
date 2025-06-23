package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/benmanns/goworker"
	"github.com/benmanns/goworker/config"
	"github.com/benmanns/goworker/redis"
)

func myFunc(queue string, args ...interface{}) error {
	fmt.Printf("From %s, %v\n", queue, args)
	return nil
}

func main() {
	// Create configuration
	cfg := config.DefaultConfig()
	cfg.Broker.Type = "redis"
	cfg.Broker.URI = "redis://localhost:6379/"
	cfg.Broker.Namespace = "resque:"
	cfg.Engine.Concurrency = 2
	cfg.Engine.Queues = []config.QueueConfig{{Name: "myqueue", Weight: 1}}
	cfg.Engine.PollInterval = 5 * time.Second
	cfg.Engine.ExitOnComplete = false
	cfg.Engine.UseNumber = true

	// Create broker
	broker := redis.NewBroker(cfg.Broker)

	// Create statistics
	stats := redis.NewStatistics(cfg.Statistics)

	// Create registry
	registry := redis.NewRegistry(cfg.Broker)

	// Create serializer
	serializer := redis.NewSerializer(cfg.Broker)

	// Create engine
	engine := goworker.NewEngine(broker, stats, registry, serializer)

	// Register job handler
	registry.Register("MyClass", myFunc)

	// Start the engine
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		log.Fatal("Error:", err)
	}
}
