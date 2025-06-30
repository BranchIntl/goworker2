package redis

import "time"

// Options for Redis broker
type Options struct {
	// URI is the Redis connection URI
	URI string

	// Namespace is the key prefix in Redis
	Namespace string

	// Queues is the list of queues to consume from
	Queues []string

	// PollInterval is the interval at which to poll for jobs
	PollInterval time.Duration

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

// ConnectionOptions interface implementation
func (o Options) GetURI() string                   { return o.URI }
func (o Options) GetMaxConnections() int           { return o.MaxConnections }
func (o Options) GetMaxIdle() int                  { return o.MaxIdle }
func (o Options) GetIdleTimeout() time.Duration    { return o.IdleTimeout }
func (o Options) GetConnectTimeout() time.Duration { return o.ConnectTimeout }
func (o Options) GetReadTimeout() time.Duration    { return o.ReadTimeout }
func (o Options) GetWriteTimeout() time.Duration   { return o.WriteTimeout }
func (o Options) GetUseTLS() bool                  { return o.UseTLS }
func (o Options) GetTLSSkipVerify() bool           { return o.TLSSkipVerify }
func (o Options) GetTLSCertPath() string           { return o.TLSCertPath }

// DefaultOptions returns default Redis options
func DefaultOptions() Options {
	return Options{
		// Redis broker options
		Namespace:    "resque:",
		Queues:       []string{},
		PollInterval: 5 * time.Second,

		// Redis connection options
		URI:            "redis://localhost:6379/",
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
