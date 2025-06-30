package core

import (
	"time"
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
		Concurrency:     25,
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
