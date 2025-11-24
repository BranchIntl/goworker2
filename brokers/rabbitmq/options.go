package rabbitmq

import "time"

// Options for RabbitMQ broker
type Options struct {
	URI           string
	PrefetchCount int
	Exchange      string
	ExchangeType  string
	Queues        []string
	// ReconnectEnabled enables automatic connection recovery
	ReconnectEnabled bool
	// ReconnectDelay is the time to wait between reconnection attempts
	ReconnectDelay time.Duration
}

// DefaultOptions returns default RabbitMQ options
func DefaultOptions() Options {
	return Options{
		URI:              "amqp://guest:guest@localhost:5672/",
		PrefetchCount:    1,
		Exchange:         "activejob",
		ExchangeType:     "direct",
		Queues:           []string{},
		ReconnectEnabled: true,
		ReconnectDelay:   5 * time.Second,
	}
}
