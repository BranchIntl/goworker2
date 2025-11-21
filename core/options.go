package core

import (
	"time"
)

const (
	// DefaultConcurrency is the default number of workers
	DefaultConcurrency = 25
	// DefaultShutdownTimeout is the default time to wait for jobs to finish
	DefaultShutdownTimeout = 30 * time.Second
	// DefaultJobBufferSize is the default size of the job channel
	DefaultJobBufferSize = 100
)

// Config holds engine configuration
type Config struct {
	Concurrency     int
	ShutdownTimeout time.Duration
	JobBufferSize   int
}

// EngineOption is a function that modifies engine configuration
type EngineOption func(*Config)

// defaultConfig returns default configuration
func defaultConfig() *Config {
	return &Config{
		Concurrency:     DefaultConcurrency,
		ShutdownTimeout: DefaultShutdownTimeout,
		JobBufferSize:   DefaultJobBufferSize,
	}
}

// WithConcurrency sets the number of concurrent workers
func WithConcurrency(n int) EngineOption {
	return func(c *Config) {
		c.Concurrency = n
	}
}

// WithShutdownTimeout sets the graceful shutdown timeout
func WithShutdownTimeout(d time.Duration) EngineOption {
	return func(c *Config) {
		c.ShutdownTimeout = d
	}
}

// WithJobBufferSize sets the job channel buffer size
func WithJobBufferSize(size int) EngineOption {
	return func(c *Config) {
		c.JobBufferSize = size
	}
}
