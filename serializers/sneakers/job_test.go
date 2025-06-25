package sneakers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/BranchIntl/goworker2/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJob(t *testing.T) {
	t.Parallel()

	queue := "test_queue"
	class := "TestJob"
	args := []interface{}{"arg1", 123, true}

	j := NewJob(queue, class, args)

	require.NotNil(t, j)
	assert.Equal(t, queue, j.GetQueue())
	assert.Equal(t, class, j.GetClass())
	assert.Equal(t, args, j.GetArgs())
	assert.NotEmpty(t, j.GetID())
	assert.False(t, j.GetEnqueuedAt().IsZero())
	assert.Equal(t, 0, j.GetRetryCount())
	assert.Empty(t, j.GetLastError())
}

func TestConstructMessage(t *testing.T) {
	t.Parallel()

	// Create a job with custom metadata
	metadata := job.Metadata{
		ID:       "test-id",
		Queue:    "test_queue",
		Priority: 5,
		Locale:   "en",
		Timezone: "UTC",
	}
	payload := job.Payload{
		Class: "TestJob",
		Args:  []interface{}{"arg1", 123},
	}

	j := &Job{metadata: metadata, payload: payload}
	msg := ConstructMessage(j)

	assert.Equal(t, "TestJob", msg.JobClass)
	assert.NotEmpty(t, msg.JobID) // Should generate new UUID
	assert.Nil(t, msg.ProviderJobID)
	assert.Equal(t, "test_queue", msg.QueueName)
	assert.Equal(t, 5, msg.Priority)
	assert.Equal(t, []interface{}{"arg1", 123}, msg.Arguments)
	assert.Equal(t, 0, msg.Executions)
	assert.Equal(t, map[string]interface{}{}, msg.ExceptionExecutions)
	assert.Equal(t, "en", msg.Locale)
	assert.Equal(t, "UTC", msg.Timezone)
	assert.NotEmpty(t, msg.EnqueuedAt)

	// Verify EnqueuedAt format
	_, err := time.Parse("2006-01-02T15:04:05Z", msg.EnqueuedAt)
	assert.NoError(t, err)
}

func TestConstructPayload(t *testing.T) {
	t.Parallel()

	msg := Message{
		JobClass:  "TestJob",
		Arguments: []interface{}{"arg1", 123, true},
	}

	payload := ConstructPayload(msg)

	assert.Equal(t, "TestJob", payload.Class)
	assert.Equal(t, []interface{}{"arg1", 123, true}, payload.Args)
}

func TestConstructMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  Message
	}{
		{
			name: "complete message",
			msg: Message{
				JobID:               "test-id",
				QueueName:           "test_queue",
				Priority:            5,
				Locale:              "en",
				Timezone:            "UTC",
				EnqueuedAt:          "2023-01-01T12:00:00Z",
				Executions:          2,
				ExceptionExecutions: map[string]interface{}{"error": "test error"},
			},
		},
		{
			name: "empty enqueued_at",
			msg: Message{
				JobID:      "test-id",
				QueueName:  "test_queue",
				EnqueuedAt: "",
			},
		},
		{
			name: "invalid enqueued_at",
			msg: Message{
				JobID:      "test-id",
				QueueName:  "test_queue",
				EnqueuedAt: "invalid-date",
			},
		},
		{
			name: "empty exception_executions",
			msg: Message{
				JobID:               "test-id",
				QueueName:           "test_queue",
				ExceptionExecutions: map[string]interface{}{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := ConstructMetadata(tt.msg)

			assert.Equal(t, tt.msg.JobID, metadata.ID)
			assert.Equal(t, tt.msg.QueueName, metadata.Queue)
			assert.Equal(t, tt.msg.Priority, metadata.Priority)
			assert.Equal(t, tt.msg.Locale, metadata.Locale)
			assert.Equal(t, tt.msg.Timezone, metadata.Timezone)
			assert.Equal(t, tt.msg.Executions, metadata.RetryCount)

			// Test EnqueuedAt parsing
			if tt.msg.EnqueuedAt == "2023-01-01T12:00:00Z" {
				expected := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
				assert.Equal(t, expected, metadata.EnqueuedAt)
			} else {
				// Should be zero time for empty or invalid dates
				assert.True(t, metadata.EnqueuedAt.IsZero())
			}

			// Test LastError from ExceptionExecutions
			if len(tt.msg.ExceptionExecutions) > 0 {
				assert.NotEmpty(t, metadata.LastError)
				// Should be valid JSON
				var parsed map[string]interface{}
				err := json.Unmarshal([]byte(metadata.LastError), &parsed)
				assert.NoError(t, err)
			} else {
				assert.Empty(t, metadata.LastError)
			}
		})
	}
}

func TestMessage_RoundTrip(t *testing.T) {
	t.Parallel()

	// Test Message -> Payload/Metadata -> back to Message equivalence
	originalMsg := Message{
		JobClass:            "TestJob",
		JobID:               "original-id",
		QueueName:           "test_queue",
		Priority:            3,
		Arguments:           []interface{}{"arg1", 123},
		Executions:          1,
		ExceptionExecutions: map[string]interface{}{"error": "test"},
		Locale:              "en",
		Timezone:            "UTC",
		EnqueuedAt:          "2023-01-01T12:00:00Z",
	}

	// Convert to metadata and payload
	metadata := ConstructMetadata(originalMsg)
	payload := ConstructPayload(originalMsg)

	// Create job
	j := &Job{metadata: metadata, payload: payload}

	// Convert back to message
	newMsg := ConstructMessage(j)

	// Compare (note: JobID will be different as ConstructMessage generates new UUID)
	assert.Equal(t, originalMsg.JobClass, newMsg.JobClass)
	assert.Equal(t, originalMsg.QueueName, newMsg.QueueName)
	assert.Equal(t, originalMsg.Priority, newMsg.Priority)
	assert.Equal(t, originalMsg.Arguments, newMsg.Arguments)
	assert.Equal(t, originalMsg.Locale, newMsg.Locale)
	assert.Equal(t, originalMsg.Timezone, newMsg.Timezone)
	// Note: Executions resets to 0 in ConstructMessage, and EnqueuedAt is regenerated
}
