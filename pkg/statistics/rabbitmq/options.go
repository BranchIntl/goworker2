package rabbitmq

import "time"

// Options for RabbitMQ statistics
type Options struct {
	// URI is the RabbitMQ connection URI (amqp://user:pass@host:port/vhost)
	URI string

	// Namespace is the key/queue prefix in RabbitMQ
	Namespace string

	// Connection pool settings
	MaxConnections int
	MaxChannels    int

	// Connection timeouts
	ConnectTimeout time.Duration
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration

	// Heartbeat interval
	Heartbeat time.Duration

	// Statistics persistence interval
	StatsPersistInterval time.Duration

	// TLS options
	UseTLS        bool
	TLSSkipVerify bool
	TLSCertPath   string
	TLSKeyPath    string
	TLSCAPath     string

	// Exchange and queue settings
	ExchangeDurable bool
	QueueDurable    bool
	QueueAutoDelete bool

	// Consumer settings
	ConsumerTag string
	AutoAck     bool
	Exclusive   bool
	NoLocal     bool
	NoWait      bool

	// Prefetch settings
	PrefetchCount  int
	PrefetchSize   int
	PrefetchGlobal bool
}

// DefaultOptions returns default RabbitMQ statistics options
func DefaultOptions() Options {
	return Options{
		URI:                  "amqp://guest:guest@localhost:5672/",
		Namespace:            "goworker:",
		MaxConnections:       10,
		MaxChannels:          100,
		ConnectTimeout:       30 * time.Second,
		WriteTimeout:         10 * time.Second,
		ReadTimeout:          10 * time.Second,
		Heartbeat:            60 * time.Second,
		StatsPersistInterval: 30 * time.Second,
		UseTLS:               false,
		TLSSkipVerify:        false,
		TLSCertPath:          "",
		TLSKeyPath:           "",
		TLSCAPath:            "",
		ExchangeDurable:      true,
		QueueDurable:         true,
		QueueAutoDelete:      false,
		ConsumerTag:          "",
		AutoAck:              false,
		Exclusive:            false,
		NoLocal:              false,
		NoWait:               false,
		PrefetchCount:        10,
		PrefetchSize:         0,
		PrefetchGlobal:       false,
	}
}
