package redis

import "time"

// Options for Redis statistics
type Options struct {
	// URI is the Redis connection URI
	URI string

	// Namespace is the key prefix in Redis
	Namespace string

	// MaxConnections is the maximum number of connections in the pool
	MaxConnections int

	// MaxIdle is the maximum number of idle connections
	MaxIdle int

	// IdleTimeout is the timeout for idle connections
	IdleTimeout time.Duration

	// ConnectTimeout is the timeout for establishing connections
	ConnectTimeout time.Duration

	// ReadTimeout is the timeout for read operations
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for write operations
	WriteTimeout time.Duration

	// TLS options
	UseTLS        bool
	TLSSkipVerify bool
	TLSCertPath   string
}

// DefaultOptions returns default Redis statistics options
func DefaultOptions() Options {
	return Options{
		URI:            "redis://localhost:6379/",
		Namespace:      "resque:",
		MaxConnections: 10,
		MaxIdle:        2,
		IdleTimeout:    240 * time.Second,
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		UseTLS:         false,
		TLSSkipVerify:  false,
		TLSCertPath:    "",
	}
}
