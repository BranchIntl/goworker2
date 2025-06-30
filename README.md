# goworker2

![Build](https://github.com/BranchIntl/goworker2/workflows/Go/badge.svg)
[![GoDoc](https://godoc.org/github.com/BranchIntl/goworker2?status.svg)](https://godoc.org/github.com/BranchIntl/goworker2)

goworker2 is a Go-based background job processing library with pluggable components. It provides a clean, modular architecture supporting multiple queue backends, serializers, and statistics providers.

Originally inspired by Resque-compatible job processing, goworker2 has evolved into a flexible framework that can work with Redis, RabbitMQ, and custom backends.

**Note**: This is a complete rewrite and modernization of the original [goworker](https://github.com/benmanns/goworker) library by Benjamin Manns, designed as a new project rather than a backwards-compatible upgrade. We're grateful for the inspiration and foundation provided by the original work.

## Features

- **Multiple Queue Backends**: Redis, RabbitMQ, or bring your own
- **Pluggable Serializers**: JSON, Resque, Sneakers/ActiveJob, or custom formats  
- **Statistics Providers**: Resque-compatible, NoOp, or custom monitoring
- **Pre-configured Engines**: Ready-to-use setups for common scenarios
- **Graceful Shutdown**: Proper signal handling and worker cleanup
- **Concurrent Processing**: Configurable worker pools with job distribution
- **Health Monitoring**: Built-in health checks and statistics

## Quick Start

### Using Pre-configured Engines

The easiest way to get started is with pre-configured engines:

#### Redis with Resque Compatibility
```go
package main

import (
	"context"
	"log"
	"time"
	
	"github.com/BranchIntl/goworker2/engines"
)

func emailJob(queue string, args ...interface{}) error {
	// Process email job
	return nil
}

func main() {
	options := engines.DefaultResqueOptions()
	options.Queues = []string{"email", "default"}
	options.PollInterval = 3 * time.Second
	
	engine := engines.NewResqueEngine(options)
	engine.Register("EmailJob", emailJob)
	
	if err := engine.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

#### RabbitMQ with ActiveJob Compatibility
```go
package main

import (
	"context"
	"log"
	
	"github.com/BranchIntl/goworker2/engines"
)

func imageProcessor(queue string, args ...interface{}) error {
	// Process image
	return nil
}

func main() {
	options := engines.DefaultSneakersOptions()
	options.Queues = []string{"images", "default"}
	
	engine := engines.NewSneakersEngine(options)
	engine.Register("ImageProcessor", imageProcessor)
	
	if err := engine.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

### Custom Configuration

For more control, you can configure components manually:

```go
package main

import (
	"context"
	"log"
	"time"
	
	"github.com/BranchIntl/goworker2/brokers/redis"
	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/registry"
	"github.com/BranchIntl/goworker2/serializers/resque"
	"github.com/BranchIntl/goworker2/statistics/resque"
)

func main() {
	// Configure broker with queues
	brokerOpts := redis.DefaultOptions()
	brokerOpts.Queues = []string{"critical", "default"}
	brokerOpts.PollInterval = 5 * time.Second
	
	// Create components
	serializer := resque.NewSerializer()
	broker := redis.NewBroker(brokerOpts, serializer)
	stats := resque.NewStatistics(resque.DefaultOptions())
	registry := registry.NewRegistry()
	
	// Create engine with custom options
	engine := core.NewEngine(
		broker,
		stats,
		registry,
		serializer,
		core.WithConcurrency(10),
		core.WithShutdownTimeout(30*time.Second),
		core.WithJobBufferSize(200),
	)
	
	// Register workers
	registry.Register("MyJob", func(queue string, args ...interface{}) error {
		// Handle job
		return nil
	})
	
	// Start processing
	if err := engine.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

## Installation

```bash
go get github.com/BranchIntl/goworker2
```

## Acknowledgments

This project was inspired by and builds upon the concepts from the original [goworker](https://github.com/benmanns/goworker) library by Benjamin Manns. While this is a complete rewrite with different architecture and capabilities, we acknowledge and appreciate the foundational work that made this project possible.

## Architecture

goworker2 uses a modular architecture with dependency injection:

```
┌─────────────────┐
│     Engine      │  ← Orchestrates components
├─────────────────┤
│   Broker        │  ← Queue backend with job consumption
│   Statistics    │  ← Metrics and monitoring
│   Registry      │  ← Worker function registry
│   Serializer    │  ← Job serialization format
│   WorkerPool    │  ← Manages concurrent workers
└─────────────────┘
```

### Components

- **Broker**: Handles queue operations and job consumption (enqueue, ack/nack, polling/pushing)
- **Statistics**: Records metrics and worker information
- **Registry**: Maps job classes to worker functions
- **Serializer**: Converts jobs to/from bytes
- **Engine**: Orchestrates all components and handles lifecycle

### Pre-configured Engines

- **ResqueEngine**: Redis + Resque serializer + Resque statistics (Ruby Resque compatibility)
- **SneakersEngine**: RabbitMQ + ActiveJob serializer + NoOp statistics (Rails ActiveJob compatibility)

See [`engines/`](engines/) directory for detailed engine documentation.

## Configuration

### Engine Options

```go
engine := core.NewEngine(
	broker, stats, registry, serializer,
	core.WithConcurrency(25),                    // Number of workers
	core.WithShutdownTimeout(30*time.Second),    // Graceful shutdown timeout
	core.WithJobBufferSize(100),                 // Job channel buffer
)
```

### Broker Options

#### Redis
```go
options := redis.DefaultOptions()
options.URI = "redis://localhost:6379/"
options.Namespace = "jobs:"
options.Queues = []string{"high", "low"}        // Queues to consume from
options.PollInterval = 5 * time.Second          // Polling frequency
options.MaxConnections = 10
```

#### RabbitMQ
```go
options := rabbitmq.DefaultOptions()
options.URI = "amqp://guest:guest@localhost:5672/"
options.Exchange = "jobs"
options.Queues = []string{"high", "low"}        // Queues to consume from
options.PrefetchCount = 1
```

## Logging

goworker2 uses Go's standard `log/slog` library for structured logging. By default, it uses the default slog logger. To customize logging, configure your logger before creating the engine using `slog.SetDefault(logger)`. For example: `slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))`.

## Worker Functions

Worker functions must match this signature:

```go
func(queue string, args ...interface{}) error
```

### Type Assertions

Use Go type assertions to handle job arguments:

```go
func processUser(queue string, args ...interface{}) error {
	if len(args) != 2 {
		return fmt.Errorf("expected 2 arguments, got %d", len(args))
	}
	
	userID, ok := args[0].(float64)  // JSON numbers are float64
	if !ok {
		return fmt.Errorf("invalid user ID type")
	}
	
	action, ok := args[1].(string)
	if !ok {
		return fmt.Errorf("invalid action type")
	}
	
	// Process user
	return processUserAction(int(userID), action)
}
```

## Signal Handling

goworker handles these signals automatically:

- **SIGINT/SIGTERM**: Graceful shutdown
- **Custom signals**: Can be handled in advanced examples

```go
// Automatic signal handling
engine.Run(ctx)  // Blocks until SIGINT/SIGTERM

// Manual control
engine.Start(ctx)
// ... custom signal handling ...
engine.Stop()
```

## Testing

For testing, use mocks or lightweight alternatives like miniredis for Redis, or run actual brokers in Docker containers for integration tests.

## Examples

Complete working examples are available in the [`examples/`](examples/) directory covering both pre-configured engines and manual component setup.

## Monitoring and Health

### Health Checks
```go
health := engine.Health()
fmt.Printf("Healthy: %v\n", health.Healthy)
fmt.Printf("Active Workers: %d\n", health.ActiveWorkers)
for queue, count := range health.QueuedJobs {
	fmt.Printf("Queue %s: %d jobs\n", queue, count)
}
```

### Statistics
```go
stats, err := engine.GetStats().GetGlobalStats(ctx)
if err == nil {
	fmt.Printf("Total Processed: %d\n", stats.TotalProcessed)
	fmt.Printf("Total Failed: %d\n", stats.TotalFailed)
}
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -am 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
