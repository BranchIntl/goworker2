package engines

import (
	"context"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/brokers/redis"
	"github.com/BranchIntl/goworker2/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResqueEngine_ConfigurationOverride(t *testing.T) {
	tests := []struct {
		name       string
		redisURI   string
		redisOpts  redis.Options
		expectType string
	}{
		{
			name:     "URI overrides options",
			redisURI: "redis://override:6380/",
			redisOpts: redis.Options{
				URI:       "redis://original:6379/",
				Namespace: "test:",
			},
			expectType: "redis",
		},
		{
			name:     "empty URI uses options",
			redisURI: "",
			redisOpts: redis.Options{
				URI:       "redis://from-options:6379/",
				Namespace: "test:",
			},
			expectType: "redis",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := ResqueOptions{
				RedisURI:      tt.redisURI,
				RedisOptions:  tt.redisOpts,
				EngineOptions: []core.EngineOption{},
			}

			engine := NewResqueEngine(options)
			assert.Equal(t, tt.expectType, engine.broker.Type())
		})
	}
}

func TestResqueEngine_ErrorHandling(t *testing.T) {
	// Use unreachable host for quick failure
	options := DefaultResqueOptions()
	options.RedisURI = "redis://unreachable-host:6379/"
	options.RedisOptions.ConnectTimeout = 100 * time.Millisecond

	engine := NewResqueEngine(options)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Test that errors propagate properly
	err := engine.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect broker")

	err = engine.Run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect broker")

	// Stop should not panic even if Start failed
	err = engine.Stop()
	assert.NoError(t, err)
}

func TestResqueEngine_MustPanicOnError(t *testing.T) {
	options := DefaultResqueOptions()
	options.RedisURI = "redis://unreachable-host:6379/"
	options.RedisOptions.ConnectTimeout = 100 * time.Millisecond

	engine := NewResqueEngine(options)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	assert.Panics(t, func() {
		engine.MustRun(ctx)
	})

	assert.Panics(t, func() {
		engine.MustStart(ctx)
	})
}
