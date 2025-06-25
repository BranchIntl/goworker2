package sneakers

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/benmanns/goworker/errors"
	"github.com/benmanns/goworker/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSneakersSerializer_Serialize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		job     job.Job
		wantErr bool
	}{
		{
			name:    "basic job",
			job:     NewJob("test_queue", "TestJob", []interface{}{"arg1", 123}),
			wantErr: false,
		},
		{
			name:    "job with no args",
			job:     NewJob("test_queue", "EmptyJob", []interface{}{}),
			wantErr: false,
		},
		{
			name:    "job with nil args",
			job:     NewJob("test_queue", "NilJob", nil),
			wantErr: false,
		},
		{
			name: "job with complex metadata",
			job: func() job.Job {
				metadata := job.Metadata{
					ID:       "custom-id",
					Queue:    "custom_queue",
					Priority: 10,
					Locale:   "fr",
					Timezone: "Europe/Paris",
				}
				payload := job.Payload{
					Class: "ComplexJob",
					Args:  []interface{}{"arg1", 123, map[string]interface{}{"key": "value"}},
				}
				return &Job{metadata: metadata, payload: payload}
			}(),
			wantErr: false,
		},
	}

	s := NewSerializer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := s.Serialize(tt.job)

			if tt.wantErr {
				require.Error(t, err)
				var serErr *errors.SerializationError
				assert.ErrorAs(t, err, &serErr)
				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, data)

			// Verify it's valid JSON
			var msg Message
			err = json.Unmarshal(data, &msg)
			require.NoError(t, err)

			// Verify basic structure
			assert.Equal(t, tt.job.GetClass(), msg.JobClass)
			assert.Equal(t, tt.job.GetQueue(), msg.QueueName)
			assert.NotEmpty(t, msg.JobID)
			assert.NotEmpty(t, msg.EnqueuedAt)

			// Compare args accounting for JSON number conversion (int -> float64)
			originalArgs := tt.job.GetArgs()
			if originalArgs != nil {
				require.Len(t, msg.Arguments, len(originalArgs))
				for i, arg := range originalArgs {
					if intVal, ok := arg.(int); ok {
						assert.Equal(t, float64(intVal), msg.Arguments[i])
					} else {
						assert.Equal(t, arg, msg.Arguments[i])
					}
				}
			} else {
				assert.Nil(t, msg.Arguments)
			}
		})
	}
}

func TestSneakersSerializer_Deserialize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		data     string
		metadata job.Metadata
		wantErr  bool
	}{
		{
			name: "valid sneakers message",
			data: `{
				"job_class": "TestJob",
				"job_id": "test-id",
				"queue_name": "test_queue",
				"priority": 5,
				"arguments": ["arg1", 123],
				"executions": 0,
				"exception_executions": {},
				"locale": "en",
				"timezone": "UTC",
				"enqueued_at": "2023-01-01T12:00:00Z"
			}`,
			metadata: job.Metadata{ID: "ignored"}, // metadata is ignored for sneakers
			wantErr:  false,
		},
		{
			name: "minimal valid message",
			data: `{
				"job_class": "MinimalJob",
				"job_id": "minimal-id",
				"queue_name": "minimal_queue",
				"arguments": []
			}`,
			metadata: job.Metadata{},
			wantErr:  false,
		},
		{
			name: "message with exception executions",
			data: `{
				"job_class": "ErrorJob",
				"job_id": "error-id",
				"queue_name": "error_queue",
				"arguments": ["arg"],
				"executions": 3,
				"exception_executions": {"RuntimeError": "Something went wrong"}
			}`,
			metadata: job.Metadata{},
			wantErr:  false,
		},
		{
			name:     "invalid json",
			data:     `{"job_class": "TestJob", "arguments": [`,
			metadata: job.Metadata{},
			wantErr:  true,
		},
		{
			name:     "empty data",
			data:     "",
			metadata: job.Metadata{},
			wantErr:  true,
		},
	}

	s := NewSerializer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j, err := s.Deserialize([]byte(tt.data), tt.metadata)

			if tt.wantErr {
				require.Error(t, err)
				var serErr *errors.SerializationError
				assert.ErrorAs(t, err, &serErr)
				assert.Equal(t, "json", serErr.Format)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, j)

			// Parse expected data to compare
			var expectedMsg Message
			err = json.Unmarshal([]byte(tt.data), &expectedMsg)
			require.NoError(t, err)

			// Verify job data matches the message
			assert.Equal(t, expectedMsg.JobClass, j.GetClass())
			assert.Equal(t, expectedMsg.QueueName, j.GetQueue())
			assert.Equal(t, expectedMsg.Arguments, j.GetArgs())
			assert.Equal(t, expectedMsg.JobID, j.GetID())
			assert.Equal(t, expectedMsg.Executions, j.GetRetryCount())

			// Verify metadata construction
			metadata := j.GetMetadata()
			assert.Equal(t, expectedMsg.JobID, metadata.ID)
			assert.Equal(t, expectedMsg.QueueName, metadata.Queue)
			assert.Equal(t, expectedMsg.Priority, metadata.Priority)
			assert.Equal(t, expectedMsg.Locale, metadata.Locale)
			assert.Equal(t, expectedMsg.Timezone, metadata.Timezone)
			assert.Equal(t, expectedMsg.Executions, metadata.RetryCount)

			// Test time parsing
			if expectedMsg.EnqueuedAt != "" {
				expectedTime, err := time.Parse("2006-01-02T15:04:05Z", expectedMsg.EnqueuedAt)
				if err == nil {
					assert.Equal(t, expectedTime, j.GetEnqueuedAt())
				}
			}

			// Test exception executions -> last error
			if len(expectedMsg.ExceptionExecutions) > 0 {
				assert.NotEmpty(t, j.GetLastError())
				// Should be valid JSON
				var parsed map[string]interface{}
				err := json.Unmarshal([]byte(j.GetLastError()), &parsed)
				assert.NoError(t, err)
			}
		})
	}
}

func TestSneakersSerializer_UseNumber_Functionality(t *testing.T) {
	t.Parallel()

	// Test that UseNumber affects deserialization of numeric values
	data := `{
		"job_class": "TestJob",
		"job_id": "test-id",
		"queue_name": "test_queue",
		"arguments": [123.45]
	}`
	metadata := job.Metadata{ID: "test-id"}

	// Test without UseNumber (default)
	s1 := NewSerializer()
	j1, err := s1.Deserialize([]byte(data), metadata)
	require.NoError(t, err)
	args1 := j1.GetArgs()
	require.Len(t, args1, 1)
	// Should be float64 by default
	assert.IsType(t, float64(0), args1[0])

	// Test with UseNumber
	s2 := NewSerializer()
	s2.SetUseNumber(true)
	j2, err := s2.Deserialize([]byte(data), metadata)
	require.NoError(t, err)
	args2 := j2.GetArgs()
	require.Len(t, args2, 1)
	// Should be json.Number when UseNumber is true
	assert.IsType(t, json.Number(""), args2[0])
}

func TestSneakersSerializer_RoundTrip(t *testing.T) {
	t.Parallel()

	// Test that serialize then deserialize preserves job data
	originalJob := NewJob("test_queue", "TestJob", []interface{}{"arg1", 123, true})
	originalJob.SetRetryCount(2)
	originalJob.SetLastError("test error")

	s := NewSerializer()

	// Serialize
	data, err := s.Serialize(originalJob)
	require.NoError(t, err)

	// Deserialize (metadata is constructed from the sneakers message)
	deserializedJob, err := s.Deserialize(data, job.Metadata{})
	require.NoError(t, err)

	// Compare essential data
	assert.Equal(t, originalJob.GetClass(), deserializedJob.GetClass())
	assert.Equal(t, originalJob.GetQueue(), deserializedJob.GetQueue())

	// Compare args accounting for JSON number conversion (int -> float64)
	originalArgs := originalJob.GetArgs()
	deserializedArgs := deserializedJob.GetArgs()
	require.Len(t, deserializedArgs, len(originalArgs))

	assert.Equal(t, "arg1", deserializedArgs[0])
	assert.Equal(t, float64(123), deserializedArgs[1]) // JSON converts int to float64
	assert.Equal(t, true, deserializedArgs[2])

	// Note: Some fields may differ due to the nature of sneakers serialization:
	// - JobID is regenerated during serialization
	// - EnqueuedAt is set to current time during serialization
	// - RetryCount and LastError handling depends on the message format
}

func TestSneakersSerializer_MetadataIgnored(t *testing.T) {
	t.Parallel()

	// Test that the metadata parameter in Deserialize is ignored
	// for sneakers format (metadata comes from the message itself)
	data := `{
		"job_class": "TestJob",
		"job_id": "message-id",
		"queue_name": "message_queue",
		"arguments": ["arg"]
	}`

	// Try with different metadata - should be ignored
	metadataParam := job.Metadata{
		ID:    "param-id",
		Queue: "param_queue",
	}

	s := NewSerializer()
	j, err := s.Deserialize([]byte(data), metadataParam)
	require.NoError(t, err)

	// Job should use data from message, not from metadata parameter
	assert.Equal(t, "message-id", j.GetID())
	assert.Equal(t, "message_queue", j.GetQueue())
	assert.Equal(t, "TestJob", j.GetClass())
	assert.Equal(t, []interface{}{"arg"}, j.GetArgs())
}
