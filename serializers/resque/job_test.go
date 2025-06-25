package resque

import (
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

func TestNewJobWithMetadata(t *testing.T) {
	t.Parallel()

	metadata := job.Metadata{
		ID:         "test-id",
		Queue:      "test_queue",
		EnqueuedAt: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
		RetryCount: 2,
		LastError:  "test error",
		Priority:   5,
	}
	payload := job.Payload{
		Class: "TestJob",
		Args:  []interface{}{"arg1", 123},
	}

	j := NewJobWithMetadata(metadata, payload)

	require.NotNil(t, j)
	assert.Equal(t, metadata.ID, j.GetID())
	assert.Equal(t, metadata.Queue, j.GetQueue())
	assert.Equal(t, payload.Class, j.GetClass())
	assert.Equal(t, payload.Args, j.GetArgs())
	assert.Equal(t, metadata.EnqueuedAt, j.GetEnqueuedAt())
	assert.Equal(t, metadata.RetryCount, j.GetRetryCount())
	assert.Equal(t, metadata.LastError, j.GetLastError())
}

func TestGenerateJobID(t *testing.T) {
	t.Parallel()

	// Test that generateJobID produces unique IDs
	id1 := generateJobID()
	id2 := generateJobID()

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2)

	// Test ID format (should contain timestamp and random number separated by hyphen)
	assert.Contains(t, id1, "-")
	assert.Contains(t, id2, "-")
}
