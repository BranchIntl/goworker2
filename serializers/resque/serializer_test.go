package resque

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/benmanns/goworker/errors"
	"github.com/benmanns/goworker/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResqueSerializer_Serialize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		job      job.Job
		expected string
	}{
		{
			name:     "basic job",
			job:      NewJob("test_queue", "TestJob", []interface{}{"arg1", 123}),
			expected: `{"class":"TestJob","args":["arg1",123]}`,
		},
		{
			name:     "job with no args",
			job:      NewJob("test_queue", "EmptyJob", []interface{}{}),
			expected: `{"class":"EmptyJob","args":[]}`,
		},
		{
			name:     "job with nil args",
			job:      NewJob("test_queue", "NilJob", nil),
			expected: `{"class":"NilJob","args":null}`,
		},
	}

	s := NewSerializer()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := s.Serialize(tt.job)
			require.NoError(t, err)
			assert.JSONEq(t, tt.expected, string(data))
		})
	}
}

func TestResqueSerializer_Deserialize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		data     string
		metadata job.Metadata
		wantErr  bool
	}{
		{
			name: "valid job data",
			data: `{"class":"TestJob","args":["arg1",123]}`,
			metadata: job.Metadata{
				ID:         "test-id",
				Queue:      "test_queue",
				EnqueuedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name: "empty args",
			data: `{"class":"EmptyJob","args":[]}`,
			metadata: job.Metadata{
				ID:    "test-id",
				Queue: "test_queue",
			},
			wantErr: false,
		},
		{
			name:     "invalid json",
			data:     `{"class":"TestJob","args":["arg1",123`,
			metadata: job.Metadata{ID: "test-id"},
			wantErr:  true,
		},
		{
			name:     "empty data",
			data:     "",
			metadata: job.Metadata{ID: "test-id"},
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

			// Verify metadata was set correctly
			assert.Equal(t, tt.metadata.ID, j.GetID())
			assert.Equal(t, tt.metadata.Queue, j.GetQueue())

			// Verify payload was deserialized correctly
			var expectedPayload job.Payload
			err = json.Unmarshal([]byte(tt.data), &expectedPayload)
			require.NoError(t, err)

			assert.Equal(t, expectedPayload.Class, j.GetClass())
			assert.Equal(t, expectedPayload.Args, j.GetArgs())
		})
	}
}

func TestResqueSerializer_UseNumber_Functionality(t *testing.T) {
	t.Parallel()

	// Test that UseNumber affects deserialization of numeric values
	data := `{"class":"TestJob","args":[123.45]}`
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

func TestResqueSerializer_RoundTrip(t *testing.T) {
	t.Parallel()

	// Test that serialize then deserialize returns equivalent job
	originalJob := NewJob("test_queue", "TestJob", []interface{}{"arg1", 123, true})
	s := NewSerializer()

	// Serialize
	data, err := s.Serialize(originalJob)
	require.NoError(t, err)

	// Deserialize
	metadata := originalJob.GetMetadata()
	deserializedJob, err := s.Deserialize(data, metadata)
	require.NoError(t, err)

	// Compare
	assert.Equal(t, originalJob.GetClass(), deserializedJob.GetClass())
	assert.Equal(t, metadata.ID, deserializedJob.GetID())
	assert.Equal(t, metadata.Queue, deserializedJob.GetQueue())

	// Compare args accounting for JSON number conversion (int -> float64)
	originalArgs := originalJob.GetArgs()
	deserializedArgs := deserializedJob.GetArgs()
	require.Len(t, deserializedArgs, len(originalArgs))

	assert.Equal(t, "arg1", deserializedArgs[0])
	assert.Equal(t, float64(123), deserializedArgs[1]) // JSON converts int to float64
	assert.Equal(t, true, deserializedArgs[2])
}
