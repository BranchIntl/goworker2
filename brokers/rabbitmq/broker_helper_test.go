package rabbitmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuildQueueArgs(t *testing.T) {
	broker := &RabbitMQBroker{}

	tests := []struct {
		name     string
		options  QueueOptions
		expected map[string]interface{}
	}{
		{
			name:     "empty options",
			options:  QueueOptions{},
			expected: map[string]interface{}{},
		},
		{
			name: "quorum queue type",
			options: QueueOptions{
				QueueType: "quorum",
			},
			expected: map[string]interface{}{
				"x-queue-type": "quorum",
			},
		},
		{
			name: "max retries sets delivery limit",
			options: QueueOptions{
				MaxRetries: 5,
			},
			expected: map[string]interface{}{
				"x-max-retries":    5,
				"x-delivery-limit": 5,
			},
		},
		{
			name: "classic queue with TTL and DLQ",
			options: QueueOptions{
				MessageTTL:      60 * time.Second,
				DeadLetterQueue: "dlq-exchange",
			},
			expected: map[string]interface{}{
				"x-message-ttl":             int64(60000),
				"x-dead-letter-exchange":    "",
				"x-dead-letter-routing-key": "dlq-exchange",
			},
		},
		{
			name: "quorum queue with max retries",
			options: QueueOptions{
				QueueType:  "quorum",
				MaxRetries: 3,
			},
			expected: map[string]interface{}{
				"x-queue-type":     "quorum",
				"x-max-retries":    3,
				"x-delivery-limit": 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := broker.buildQueueArgs(tt.options)

			// Check that all expected keys exist and match
			for k, v := range tt.expected {
				assert.Equal(t, v, args[k], "Value mismatch for key %s", k)
			}

			// Check that no extra keys exist
			assert.Equal(t, len(tt.expected), len(args), "Unexpected number of arguments")
		})
	}
}
