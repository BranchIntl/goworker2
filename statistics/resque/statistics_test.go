package resque

import (
	"context"
	"testing"
	"time"

	"github.com/benmanns/goworker/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper functions following the redis package pattern
func unreachableOpts(uri string) Options {
	opts := DefaultOptions()
	opts.URI = uri
	opts.ConnectTimeout = 100 * time.Millisecond // Fail fast
	return opts
}

func assertConnError(t *testing.T, err error) {
	require.Error(t, err)
	var connErr *errors.ConnectionError
	assert.ErrorAs(t, err, &connErr)
}

func TestResqueStatistics_Connect(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{"unreachable redis", unreachableOpts("redis://unreachable-host:6379")},
		{"unreachable rediss", unreachableOpts("rediss://unreachable-host:6380")},
		{"invalid URI", unreachableOpts(":/invalid-uri")},
		{"unsupported scheme", unreachableOpts("http://localhost:6379")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := NewStatistics(tt.opts)
			ctx := context.Background()

			err := stats.Connect(ctx)
			assertConnError(t, err)
		})
	}
}

func TestResqueStatistics_Health_NotConnected(t *testing.T) {
	opts := DefaultOptions()
	stats := NewStatistics(opts)

	err := stats.Health()
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestResqueStatistics_NilPoolOperations(t *testing.T) {
	stats := NewStatistics(Options{
		URI:       "redis://localhost:6379",
		Namespace: "test:",
	})
	// Don't call Connect() so pool remains nil

	t.Run("Health with nil pool", func(t *testing.T) {
		err := stats.Health()
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrNotConnected)
	})

	t.Run("Close with nil pool", func(t *testing.T) {
		// Close should handle nil pool gracefully
		err := stats.Close()
		assert.NoError(t, err)
	})
}

func TestResqueStatistics_ConnectionEdgeCases(t *testing.T) {
	t.Run("Connect with invalid Redis URI", func(t *testing.T) {
		stats := NewStatistics(Options{
			URI:            "invalid://uri",
			Namespace:      "test:",
			ConnectTimeout: 100 * time.Millisecond,
		})

		ctx := context.Background()
		err := stats.Connect(ctx)
		assert.Error(t, err)
		var connErr *errors.ConnectionError
		assert.ErrorAs(t, err, &connErr)
	})

	t.Run("Health after failed connect", func(t *testing.T) {
		stats := NewStatistics(Options{
			URI:            "redis://unreachable-host:6379",
			Namespace:      "test:",
			ConnectTimeout: 100 * time.Millisecond,
		})

		// Try to connect (will fail)
		ctx := context.Background()
		err := stats.Connect(ctx)
		assert.Error(t, err)

		// Health should return a connection error since the pool was created but connection fails
		err = stats.Health()
		assert.Error(t, err)
		var connErr *errors.ConnectionError
		assert.ErrorAs(t, err, &connErr)
	})
}
