# Examples

Working examples demonstrating different goworker configurations.

## Quick Start

| Example | Engine | Description |
|---------|--------|-------------|
| **redis-basic/** | ResqueEngine | Minimal Redis/Resque setup |
| **rabbitmq-basic/** | SneakersEngine | Minimal RabbitMQ/ActiveJob setup |

## Advanced

| Example | Components | Description |
|---------|------------|-------------|
| **redis-advanced/** | Manual Redis | Custom connections, monitoring |
| **rabbitmq-advanced/** | Manual RabbitMQ | Mixed backends, queue management |

## Running

```bash
# Prerequisites: Redis on :6379 and/or RabbitMQ on :5672
cd redis-basic && go run main.go
cd rabbitmq-basic && go run main.go
cd redis-advanced && go run main.go
cd rabbitmq-advanced && go run main.go
```

## Testing Jobs

**Redis:**
```bash
redis-cli RPUSH resque:queue:myqueue '{"class":"MyClass","args":["hello","world"]}'
```

**RabbitMQ (via Rails):**
```ruby
MyClass.perform_later("hello", "world")
```

## Signals

| Signal | Basic | Advanced |
|--------|-------|----------|
| `Ctrl+C`, `SIGTERM` | Shutdown | Shutdown |
| `SIGUSR1` | - | Health status |
| `SIGUSR2` | - | Queue stats |

```bash
# Advanced examples only
kill -USR1 <pid>  # Health
kill -USR2 <pid>  # Stats (rabbitmq-advanced only)
``` 