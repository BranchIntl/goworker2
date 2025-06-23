package config

import (
	"fmt"
	"os"
	"time"
)

// Config is the main configuration structure
type Config struct {
	Broker     BrokerConfig     `json:"broker" yaml:"broker"`
	Statistics StatisticsConfig `json:"statistics" yaml:"statistics"`
	Engine     EngineConfig     `json:"engine" yaml:"engine"`
	Logging    LoggingConfig    `json:"logging" yaml:"logging"`
}

// BrokerConfig contains broker-specific configuration
type BrokerConfig struct {
	Type      string                 `json:"type" yaml:"type"`
	URI       string                 `json:"uri" yaml:"uri"`
	Namespace string                 `json:"namespace" yaml:"namespace"`
	Options   map[string]interface{} `json:"options" yaml:"options"`
}

// StatisticsConfig contains statistics-specific configuration
type StatisticsConfig struct {
	Type      string                 `json:"type" yaml:"type"`
	URI       string                 `json:"uri" yaml:"uri"`
	Namespace string                 `json:"namespace" yaml:"namespace"`
	Options   map[string]interface{} `json:"options" yaml:"options"`
}

// EngineConfig contains engine-specific configuration
type EngineConfig struct {
	Concurrency      int           `json:"concurrency" yaml:"concurrency"`
	Queues           []QueueConfig `json:"queues" yaml:"queues"`
	PollInterval     time.Duration `json:"poll_interval" yaml:"poll_interval"`
	ShutdownTimeout  time.Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`
	ExitOnComplete   bool          `json:"exit_on_complete" yaml:"exit_on_complete"`
	StrictQueueOrder bool          `json:"strict_queue_order" yaml:"strict_queue_order"`
	UseNumber        bool          `json:"use_number" yaml:"use_number"`
}

// QueueConfig contains queue-specific configuration
type QueueConfig struct {
	Name     string `json:"name" yaml:"name"`
	Weight   int    `json:"weight" yaml:"weight"`
	Priority int    `json:"priority" yaml:"priority"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level  string `json:"level" yaml:"level"`
	Format string `json:"format" yaml:"format"`
	Output string `json:"output" yaml:"output"`
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Broker: BrokerConfig{
			Type:      "redis",
			URI:       getEnvOrDefault("REDIS_URL", "redis://localhost:6379/"),
			Namespace: "resque:",
			Options:   make(map[string]interface{}),
		},
		Statistics: StatisticsConfig{
			Type:      "redis",
			URI:       getEnvOrDefault("REDIS_URL", "redis://localhost:6379/"),
			Namespace: "resque:",
			Options:   make(map[string]interface{}),
		},
		Engine: EngineConfig{
			Concurrency:      25,
			Queues:           []QueueConfig{{Name: "default", Weight: 1}},
			PollInterval:     5 * time.Second,
			ShutdownTimeout:  30 * time.Second,
			ExitOnComplete:   false,
			StrictQueueOrder: true,
			UseNumber:        false,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
			Output: "stdout",
		},
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Broker.Type == "" {
		return fmt.Errorf("broker type is required")
	}

	if c.Statistics.Type == "" {
		return fmt.Errorf("statistics type is required")
	}

	if c.Engine.Concurrency < 1 {
		return fmt.Errorf("concurrency must be at least 1")
	}

	if len(c.Engine.Queues) == 0 {
		return fmt.Errorf("at least one queue must be configured")
	}

	for _, queue := range c.Engine.Queues {
		if queue.Name == "" {
			return fmt.Errorf("queue name cannot be empty")
		}
		if queue.Weight < 1 {
			queue.Weight = 1
		}
	}

	return nil
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	// Check REDIS_PROVIDER
	if key == "REDIS_URL" {
		if provider := os.Getenv("REDIS_PROVIDER"); provider != "" {
			if value := os.Getenv(provider); value != "" {
				return value
			}
		}
	}
	return defaultValue
}
