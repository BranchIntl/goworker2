package core

import (
	"time"
)

// Config holds engine configuration
type Config struct {
	Concurrency     int
	Queues          []string
	PollInterval    time.Duration
	ShutdownTimeout time.Duration
	JobBufferSize   int
}

// EngineOption is a function that modifies engine configuration
type EngineOption func(*Config)

// defaultConfig returns default configuration
func defaultConfig() *Config {
	return &Config{
		Concurrency:     25,
		Queues:          []string{},
		PollInterval:    5 * time.Second,
		ShutdownTimeout: 30 * time.Second,
		JobBufferSize:   100,
	}
}

// WithConcurrency sets the number of concurrent workers
func WithConcurrency(n int) EngineOption {
	return func(c *Config) {
		c.Concurrency = n
	}
}

// WithQueues sets the queues to process
func WithQueues(queues []string) EngineOption {
	return func(c *Config) {
		c.Queues = queues
	}
}

// WithPollInterval sets the polling interval
func WithPollInterval(d time.Duration) EngineOption {
	return func(c *Config) {
		c.PollInterval = d
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
