package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMultipleOptions(t *testing.T) {
	config := defaultConfig()

	// Apply multiple options
	options := []EngineOption{
		WithConcurrency(15),
		WithShutdownTimeout(60 * time.Second),
		WithJobBufferSize(500),
	}

	for _, option := range options {
		option(config)
	}

	// Verify all options were applied
	assert.Equal(t, 15, config.Concurrency)
	assert.Equal(t, 60*time.Second, config.ShutdownTimeout)
	assert.Equal(t, 500, config.JobBufferSize)
}
