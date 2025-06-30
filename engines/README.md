# Pre-configured Engines

This package provides ready-to-use engine configurations that automatically wire together compatible components.

## ResqueEngine

Redis-based engine with Ruby Resque compatibility.

**Configuration:**
- **Broker:** Redis 
- **Serializer:** Resque JSON format
- **Statistics:** Resque (Redis-based)

**Options:**
```go
type ResqueOptions struct {
    RedisURI      string               // Default: redis://localhost:6379/
    RedisOptions  redis.Options        // Detailed Redis configuration
    Queues        []string             // Queues to consume from
    PollInterval  time.Duration        // Polling interval for jobs
    EngineOptions []core.EngineOption
}
```

## SneakersEngine

RabbitMQ-based engine with Rails ActiveJob compatibility.

**Configuration:**
- **Broker:** RabbitMQ
- **Serializer:** ActiveJob/Sneakers JSON format
- **Statistics:** NoOp (configurable)

**Options:**
```go
type SneakersOptions struct {
    RabbitMQURI     string                // Default: amqp://guest:guest@localhost:5672/
    RabbitMQOptions rabbitmq.Options      // Detailed RabbitMQ configuration
    Queues          []string              // Queues to consume from
    Statistics      core.Statistics       // Optional custom statistics
    EngineOptions   []core.EngineOption
}
```

## API

All pre-configured engines provide:

| Method | Description |
|--------|-------------|
| `NewXXXEngine(options)` | Create engine with options |
| `Register(class, worker)` | Register worker function |
| `Run(ctx)` | Start with signal handling |
| `Start(ctx)` / `Stop()` | Manual lifecycle control |
| `MustRun(ctx)` / `MustStart(ctx)` | Panic on error variants |

**Component Access:**
| Method | Returns |
|--------|---------|
| `GetBroker()` | Broker instance |
| `GetStats()` | Statistics backend |
| `GetRegistry()` | Worker registry |
| `GetSerializer()` | Serializer |

## Customization

Configure queues for ResqueEngine:
```go
options := engines.DefaultResqueOptions()
options.Queues = []string{"high", "medium", "low"}
options.PollInterval = 3 * time.Second
engine := engines.NewResqueEngine(options)
```

Configure queues and statistics for SneakersEngine:
```go
options := engines.DefaultSneakersOptions()
options.Queues = []string{"activejob", "images"}
options.Statistics = resque.NewStatistics(resque.DefaultOptions())
engine := engines.NewSneakersEngine(options)
```

See the main README for usage examples and the `examples/` directory for complete implementations. 