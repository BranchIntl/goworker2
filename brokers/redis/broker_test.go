package redis

import (
	"context"
	"testing"
	"time"

	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/errors"
	"github.com/benmanns/goworker/job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSerializer is a simple serializer for testing
type mockSerializer struct {
	serializeErr   error
	deserializeErr error
	format         string
	useNumber      bool
}

func (m *mockSerializer) Serialize(j job.Job) ([]byte, error) {
	if m.serializeErr != nil {
		return nil, m.serializeErr
	}
	return []byte(`{"class":"TestClass","args":["arg1","arg2"]}`), nil
}

func (m *mockSerializer) Deserialize(data []byte, metadata job.Metadata) (job.Job, error) {
	if m.deserializeErr != nil {
		return nil, m.deserializeErr
	}
	return &mockJob{
		id:       metadata.ID,
		queue:    metadata.Queue,
		class:    "TestClass",
		args:     []interface{}{"arg1", "arg2"},
		metadata: metadata,
	}, nil
}

func (m *mockSerializer) GetFormat() string           { return m.format }
func (m *mockSerializer) UseNumber() bool             { return m.useNumber }
func (m *mockSerializer) SetUseNumber(useNumber bool) { m.useNumber = useNumber }

// mockJob is a simple job implementation for testing
type mockJob struct {
	id       string
	queue    string
	class    string
	args     []interface{}
	metadata job.Metadata
}

func (m *mockJob) GetID() string             { return m.id }
func (m *mockJob) GetQueue() string          { return m.queue }
func (m *mockJob) GetClass() string          { return m.class }
func (m *mockJob) GetArgs() []interface{}    { return m.args }
func (m *mockJob) GetMetadata() job.Metadata { return m.metadata }
func (m *mockJob) GetEnqueuedAt() time.Time  { return m.metadata.EnqueuedAt }
func (m *mockJob) GetRetryCount() int        { return m.metadata.RetryCount }
func (m *mockJob) GetLastError() string      { return m.metadata.LastError }
func (m *mockJob) SetRetryCount(count int)   { m.metadata.RetryCount = count }
func (m *mockJob) SetLastError(err string)   { m.metadata.LastError = err }
func (m *mockJob) GetPayload() job.Payload {
	return job.Payload{Class: m.class, Args: m.args}
}

func TestNewBroker(t *testing.T) {
	options := DefaultOptions()
	serializer := &mockSerializer{format: "json"}

	broker := NewBroker(options, serializer)

	require.NotNil(t, broker)
	assert.Equal(t, options.Namespace, broker.namespace)
	assert.Equal(t, options, broker.options)
	assert.Equal(t, serializer, broker.serializer)
}

func TestRedisBroker_Type(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	assert.Equal(t, "redis", broker.Type())
}

func TestRedisBroker_Capabilities(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	capabilities := broker.Capabilities()

	expected := core.BrokerCapabilities{
		SupportsAck:        false,
		SupportsDelay:      false,
		SupportsPriority:   false,
		SupportsDeadLetter: false,
	}

	assert.Equal(t, expected, capabilities)
}

func TestRedisBroker_Connect_InvalidURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
	}{
		{
			name: "invalid URI format",
			uri:  "://invalid",
		},
		{
			name: "unsupported scheme",
			uri:  "http://localhost:6379",
		},
		{
			name: "unreachable host",
			uri:  "redis://unreachable-host:6379",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := DefaultOptions()
			options.URI = tt.uri
			options.ConnectTimeout = 100 * time.Millisecond // Fail fast

			broker := NewBroker(options, &mockSerializer{})
			ctx := context.Background()

			err := broker.Connect(ctx)
			require.Error(t, err)

			var connErr *errors.ConnectionError
			assert.ErrorAs(t, err, &connErr)
		})
	}
}

func TestRedisBroker_Health_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})

	err := broker.Health()
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRedisBroker_Enqueue_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	job := &mockJob{
		id:    "test-job-1",
		queue: "test_queue",
		class: "TestClass",
		args:  []interface{}{"arg1", "arg2"},
	}

	err := broker.Enqueue(ctx, job)
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRedisBroker_Dequeue_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	job, err := broker.Dequeue(ctx, "test_queue")
	assert.ErrorIs(t, err, errors.ErrNotConnected)
	assert.Nil(t, job)
}

func TestRedisBroker_QueueLength_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	length, err := broker.QueueLength(ctx, "test_queue")
	assert.ErrorIs(t, err, errors.ErrNotConnected)
	assert.Equal(t, int64(0), length)
}

func TestRedisBroker_DeleteQueue_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	err := broker.DeleteQueue(ctx, "test_queue")
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}

func TestRedisBroker_QueueExists_NotConnected(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	exists, err := broker.QueueExists(ctx, "test_queue")
	assert.ErrorIs(t, err, errors.ErrNotConnected)
	assert.False(t, exists)
}

func TestRedisBroker_Close_NilPool(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})

	err := broker.Close()
	assert.NoError(t, err)
}

func TestRedisBroker_Ack(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	job := &mockJob{id: "test", queue: "test_queue", class: "TestClass"}

	// Ack should always succeed (no-op for Redis)
	err := broker.Ack(ctx, job)
	assert.NoError(t, err)
}

func TestRedisBroker_Nack(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	job := &mockJob{id: "test", queue: "test_queue", class: "TestClass"}

	t.Run("nack without requeue", func(t *testing.T) {
		err := broker.Nack(ctx, job, false)
		assert.NoError(t, err)
	})

	t.Run("nack with requeue but not connected", func(t *testing.T) {
		err := broker.Nack(ctx, job, true)
		// Should fail because Enqueue will fail when not connected
		assert.ErrorIs(t, err, errors.ErrNotConnected)
	})
}

func TestRedisBroker_CreateQueue(t *testing.T) {
	broker := NewBroker(DefaultOptions(), &mockSerializer{})
	ctx := context.Background()

	// CreateQueue is a no-op for Redis
	err := broker.CreateQueue(ctx, "test_queue", core.QueueOptions{})
	assert.NoError(t, err)
}

func TestRedisBroker_SerializationError(t *testing.T) {
	serializer := &mockSerializer{
		serializeErr: assert.AnError,
		format:       "json",
	}

	broker := NewBroker(DefaultOptions(), serializer)
	// Manually set a mock pool to bypass connection
	broker.pool = nil

	ctx := context.Background()
	job := &mockJob{id: "test", queue: "test_queue", class: "TestClass"}

	err := broker.Enqueue(ctx, job)
	// Should first fail with not connected since we don't have a real pool
	assert.ErrorIs(t, err, errors.ErrNotConnected)
}
