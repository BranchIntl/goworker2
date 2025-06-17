package rabbitmq

// Options for RabbitMQ broker
type Options struct {
	URI           string
	PrefetchCount int
	Exchange      string
	ExchangeType  string
}

// DefaultOptions returns default RabbitMQ options
func DefaultOptions() Options {
	return Options{
		URI:           "amqp://guest:guest@localhost:5672/",
		PrefetchCount: 1,
		Exchange:      "",
		ExchangeType:  "direct",
	}
}
