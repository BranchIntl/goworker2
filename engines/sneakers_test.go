package engines

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benmanns/goworker/brokers/rabbitmq"
	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/statistics/noop"
	"github.com/benmanns/goworker/statistics/resque"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSneakersEngine_ConfigurationOverride(t *testing.T) {
	tests := []struct {
		name         string
		rabbitMQURI  string
		rabbitMQOpts rabbitmq.Options
		expectType   string
	}{
		{
			name:        "URI overrides options",
			rabbitMQURI: "amqp://override:password@override-host:5672/",
			rabbitMQOpts: rabbitmq.Options{
				URI:           "amqp://original:password@original-host:5672/",
				PrefetchCount: 5,
				Exchange:      "test-exchange",
			},
			expectType: "rabbitmq",
		},
		{
			name:        "empty URI uses options",
			rabbitMQURI: "",
			rabbitMQOpts: rabbitmq.Options{
				URI:           "amqp://from-options:password@options-host:5672/",
				PrefetchCount: 10,
				Exchange:      "options-exchange",
			},
			expectType: "rabbitmq",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := SneakersOptions{
				RabbitMQURI:     tt.rabbitMQURI,
				RabbitMQOptions: tt.rabbitMQOpts,
				Statistics:      noop.NewStatistics(),
				EngineOptions:   []core.EngineOption{},
			}

			engine := NewSneakersEngine(options)
			assert.Equal(t, tt.expectType, engine.broker.Type())
		})
	}
}

func TestSneakersEngine_StatisticsHandling(t *testing.T) {
	tests := []struct {
		name       string
		statistics core.Statistics
		expectType string
	}{
		{
			name:       "nil statistics becomes noop",
			statistics: nil,
			expectType: "*noop.NoOpStatistics",
		},
		{
			name:       "custom statistics preserved",
			statistics: resque.NewStatistics(resque.DefaultOptions()),
			expectType: "*resque.ResqueStatistics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := DefaultSneakersOptions()
			options.Statistics = tt.statistics

			engine := NewSneakersEngine(options)

			require.NotNil(t, engine.stats)
			assert.Contains(t, fmt.Sprintf("%T", engine.stats), tt.expectType)
		})
	}
}

func TestSneakersEngine_ErrorHandling(t *testing.T) {
	// Use unreachable host for quick failure
	options := DefaultSneakersOptions()
	options.RabbitMQURI = "amqp://unreachable-host:5672/"

	engine := NewSneakersEngine(options)

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

func TestSneakersEngine_MustPanicOnError(t *testing.T) {
	options := DefaultSneakersOptions()
	options.RabbitMQURI = "amqp://unreachable-host:5672/"

	engine := NewSneakersEngine(options)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	assert.Panics(t, func() {
		engine.MustRun(ctx)
	})

	assert.Panics(t, func() {
		engine.MustStart(ctx)
	})
}
