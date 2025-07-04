// Package goworker2 provides a Go-based background job processing library
// with pluggable components and modular architecture.
//
// Originally inspired by Resque-compatible job processing, goworker2 has evolved
// into a flexible framework supporting multiple queue backends (Redis, RabbitMQ),
// serializers (JSON, Resque, Sneakers/ActiveJob), and statistics providers.
//
// # Architecture
//
// goworker uses dependency injection with these core components:
//   - Broker: Handles queue operations (Redis, RabbitMQ)
//   - Statistics: Records metrics and monitoring data
//   - Registry: Maps job classes to worker functions
//   - Serializer: Converts jobs to/from bytes
//   - Engine: Orchestrates all components and handles lifecycle
//
// # Quick Start with Pre-configured Engines
//
// For Resque compatibility with Redis:
//
//	import "github.com/BranchIntl/goworker2/engines"
//
//	func emailJob(queue string, args ...interface{}) error {
//		// Process email job
//		return nil
//	}
//
//	func main() {
//		engine := engines.NewResqueEngine(engines.DefaultResqueOptions())
//		engine.Register("EmailJob", emailJob)
//		engine.Run(context.Background())
//	}
//
// For ActiveJob compatibility with RabbitMQ:
//
//	import "github.com/BranchIntl/goworker2/engines"
//
//	func imageProcessor(queue string, args ...interface{}) error {
//		// Process image
//		return nil
//	}
//
//	func main() {
//		engine := engines.NewSneakersEngine(engines.DefaultSneakersOptions())
//		engine.Register("ImageProcessor", imageProcessor)
//		engine.Run(context.Background())
//	}
//
// # Custom Configuration
//
// For complete control over components:
//
//	import (
//		"context"
//		"github.com/BranchIntl/goworker2/brokers/redis"
//		"github.com/BranchIntl/goworker2/core"
//		"github.com/BranchIntl/goworker2/registry"
//		"github.com/BranchIntl/goworker2/serializers/resque"
//		"github.com/BranchIntl/goworker2/statistics/resque"
//	)
//
//	func main() {
//		// Create components
//		broker := redis.NewBroker(redis.DefaultOptions(), resque.NewSerializer())
//		stats := resque.NewStatistics(resque.DefaultOptions())
//		reg := registry.NewRegistry()
//		serializer := resque.NewSerializer()
//
//		// Create engine
//		engine := core.NewEngine(
//			broker,    // implements core.Broker
//			stats,     // implements core.Statistics
//			reg,       // implements core.Registry
//			serializer, // implements core.Serializer
//			core.WithConcurrency(10),
//			core.WithQueues([]string{"critical", "default"}),
//		)
//
//		// Register workers
//		reg.Register("EmailJob", sendEmail)
//
//		// Start processing
//		engine.Run(context.Background())
//	}
//
// # Worker Functions
//
// Worker functions must match this signature:
//
//	func(queue string, args ...interface{}) error
//
// Use type assertions to handle arguments:
//
//	func processUser(queue string, args ...interface{}) error {
//		userID, ok := args[0].(float64)  // JSON numbers are float64
//		if !ok {
//			return fmt.Errorf("invalid user ID")
//		}
//		// Process user...
//		return nil
//	}
//
// # Signal Handling
//
// The engine.Run() method automatically handles SIGINT and SIGTERM for graceful
// shutdown. For manual control:
//
//	engine.Start(ctx)
//	// Custom signal handling...
//	engine.Stop()
//
// # Testing
//
// For testing, use mocks or lightweight alternatives like miniredis for Redis,
// or run actual brokers in Docker containers for integration tests.
//
// # Available Engines
//
// ResqueEngine: Redis + Resque serializer + Resque statistics
// - Compatible with Ruby Resque
// - Uses Redis for queuing and statistics
//
// SneakersEngine: RabbitMQ + Sneakers serializer + NoOp statistics
// - Compatible with Rails ActiveJob/Sneakers
// - Uses RabbitMQ for queuing
//
// # Health Monitoring
//
//	health := engine.Health()
//	if health.Healthy {
//		fmt.Printf("Active workers: %d\n", health.ActiveWorkers)
//		for queue, count := range health.QueuedJobs {
//			fmt.Printf("Queue %s: %d jobs\n", queue, count)
//		}
//	}
package goworker2
