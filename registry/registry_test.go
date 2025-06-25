package registry

import (
	"testing"

	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test worker functions for testing
func testWorker1(queue string, args ...interface{}) error {
	return nil
}

func testWorker2(queue string, args ...interface{}) error {
	return nil
}

func TestRegistry_Register(t *testing.T) {
	tests := []struct {
		name      string
		class     string
		worker    core.WorkerFunc
		expectErr error
	}{
		{
			name:      "valid registration",
			class:     "TestWorker",
			worker:    testWorker1,
			expectErr: nil,
		},
		{
			name:      "empty class name",
			class:     "",
			worker:    testWorker1,
			expectErr: errors.ErrEmptyClassName,
		},
		{
			name:      "nil worker function",
			class:     "TestWorker",
			worker:    nil,
			expectErr: errors.ErrNilWorkerFunc,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := NewRegistry()

			err := registry.Register(tt.class, tt.worker)

			if tt.expectErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.expectErr)
			} else {
				require.NoError(t, err)

				// Verify registration worked
				worker, found := registry.Get(tt.class)
				assert.True(t, found)
				assert.NotNil(t, worker)
			}
		})
	}
}

func TestRegistry_BasicOperations(t *testing.T) {
	registry := NewRegistry()

	// Register workers
	err := registry.Register("Worker1", testWorker1)
	require.NoError(t, err)

	err = registry.Register("Worker2", testWorker2)
	require.NoError(t, err)

	// Test Get
	worker, found := registry.Get("Worker1")
	assert.True(t, found)
	assert.NotNil(t, worker)

	_, found = registry.Get("NonExistent")
	assert.False(t, found)

	// Test List
	classes := registry.List()
	assert.Len(t, classes, 2)
	assert.Contains(t, classes, "Worker1")
	assert.Contains(t, classes, "Worker2")

	// Test Remove
	err = registry.Remove("Worker1")
	require.NoError(t, err)

	_, found = registry.Get("Worker1")
	assert.False(t, found)

	classes = registry.List()
	assert.Len(t, classes, 1)

	// Test Clear
	registry.Clear()
	assert.Empty(t, registry.List())
}
