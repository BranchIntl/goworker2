package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/gomodule/redigo/redis"
)

// RedisBroker implements the Broker interface for Redis
type RedisBroker struct {
	pool       *redis.Pool
	namespace  string
	options    Options
	serializer interfaces.Serializer
}

// NewBroker creates a new Redis broker
func NewBroker(options Options, serializer interfaces.Serializer) *RedisBroker {
	return &RedisBroker{
		namespace:  options.Namespace,
		options:    options,
		serializer: serializer,
	}
}

// Connect establishes connection to Redis
func (r *RedisBroker) Connect(ctx context.Context) error {
	pool, err := createPool(r.options)
	if err != nil {
		return fmt.Errorf("failed to create Redis pool: %w", err)
	}

	r.pool = pool

	// Test connection
	conn := r.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("PING"); err != nil {
		return fmt.Errorf("failed to ping Redis: %w", err)
	}

	return nil
}

// Close closes the Redis connection pool
func (r *RedisBroker) Close() error {
	if r.pool != nil {
		return r.pool.Close()
	}
	return nil
}

// Health checks the Redis connection health
func (r *RedisBroker) Health() error {
	if r.pool == nil {
		return fmt.Errorf("not connected")
	}

	conn := r.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("PING"); err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// Type returns the broker type
func (r *RedisBroker) Type() string {
	return "redis"
}

// Capabilities returns Redis broker capabilities
func (r *RedisBroker) Capabilities() interfaces.BrokerCapabilities {
	return interfaces.BrokerCapabilities{
		SupportsAck:        false, // Redis doesn't have built-in ACK
		SupportsDelay:      false, // Could be implemented with sorted sets
		SupportsPriority:   false, // Could be implemented with multiple queues
		SupportsDeadLetter: false, // Could be implemented
	}
}

// Enqueue adds a job to the queue
func (r *RedisBroker) Enqueue(ctx context.Context, job interfaces.Job) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Serialize job
	data, err := r.serializer.Serialize(job)
	if err != nil {
		return fmt.Errorf("failed to serialize job: %w", err)
	}

	queueKey := r.queueKey(job.GetQueue())

	// Add to queue
	if _, err := conn.Do("RPUSH", queueKey, data); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	// Add queue to set of known queues
	if _, err := conn.Do("SADD", r.queuesKey(), job.GetQueue()); err != nil {
		return fmt.Errorf("failed to register queue: %w", err)
	}

	return nil
}

// Dequeue retrieves a job from the queue
func (r *RedisBroker) Dequeue(ctx context.Context, queue string) (interfaces.Job, error) {
	conn := r.pool.Get()
	defer conn.Close()

	queueKey := r.queueKey(queue)

	// Pop from queue
	reply, err := conn.Do("LPOP", queueKey)
	if err != nil {
		return nil, fmt.Errorf("failed to dequeue: %w", err)
	}

	if reply == nil {
		return nil, nil // No job available
	}

	data, ok := reply.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid data type from Redis")
	}

	// Create metadata
	metadata := interfaces.JobMetadata{
		Queue:      queue,
		EnqueuedAt: time.Now(), // Redis doesn't store this
	}

	// Deserialize job
	job, err := r.serializer.Deserialize(data, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize job: %w", err)
	}

	return job, nil
}

// Ack acknowledges job completion (no-op for Redis)
func (r *RedisBroker) Ack(ctx context.Context, job interfaces.Job) error {
	// Redis doesn't have built-in ACK mechanism
	// Job is already removed from queue by LPOP
	return nil
}

// Nack rejects a job and optionally requeues it
func (r *RedisBroker) Nack(ctx context.Context, job interfaces.Job, requeue bool) error {
	if !requeue {
		return nil // Nothing to do
	}

	// Requeue the job
	return r.Enqueue(ctx, job)
}

// CreateQueue creates a new queue (no-op for Redis)
func (r *RedisBroker) CreateQueue(ctx context.Context, name string, options interfaces.QueueOptions) error {
	// Redis queues are created on-demand
	return nil
}

// DeleteQueue deletes a queue
func (r *RedisBroker) DeleteQueue(ctx context.Context, name string) error {
	conn := r.pool.Get()
	defer conn.Close()

	queueKey := r.queueKey(name)

	// Delete the queue
	if _, err := conn.Do("DEL", queueKey); err != nil {
		return fmt.Errorf("failed to delete queue: %w", err)
	}

	// Remove from set of queues
	if _, err := conn.Do("SREM", r.queuesKey(), name); err != nil {
		return fmt.Errorf("failed to unregister queue: %w", err)
	}

	return nil
}

// QueueExists checks if a queue exists
func (r *RedisBroker) QueueExists(ctx context.Context, name string) (bool, error) {
	conn := r.pool.Get()
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("SISMEMBER", r.queuesKey(), name))
	if err != nil {
		return false, fmt.Errorf("failed to check queue existence: %w", err)
	}

	return exists, nil
}

// QueueLength returns the number of jobs in a queue
func (r *RedisBroker) QueueLength(ctx context.Context, name string) (int64, error) {
	conn := r.pool.Get()
	defer conn.Close()

	queueKey := r.queueKey(name)

	length, err := redis.Int64(conn.Do("LLEN", queueKey))
	if err != nil {
		return 0, fmt.Errorf("failed to get queue length: %w", err)
	}

	return length, nil
}

// Helper methods

func (r *RedisBroker) queueKey(queue string) string {
	return fmt.Sprintf("%squeue:%s", r.namespace, queue)
}

func (r *RedisBroker) queuesKey() string {
	return fmt.Sprintf("%squeues", r.namespace)
}
