package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/errors"
	redisUtils "github.com/BranchIntl/goworker2/internal/redis"
	"github.com/BranchIntl/goworker2/job"
	"github.com/cihub/seelog"
	"github.com/gomodule/redigo/redis"
)

// RedisBroker implements the Broker interface for Redis
type RedisBroker struct {
	pool       *redis.Pool
	namespace  string
	options    Options
	serializer core.Serializer
	logger     seelog.LoggerInterface
}

// NewBroker creates a new Redis broker
func NewBroker(options Options, serializer core.Serializer) *RedisBroker {
	return &RedisBroker{
		namespace:  options.Namespace,
		options:    options,
		serializer: serializer,
	}
}

// Connect establishes connection to Redis
func (r *RedisBroker) Connect(ctx context.Context) error {
	pool, err := redisUtils.CreatePool(r.options)
	if err != nil {
		return errors.NewConnectionError(r.options.URI,
			fmt.Errorf("failed to create Redis pool: %w", err))
	}

	r.pool = pool

	// Test connection
	conn := r.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("PING"); err != nil {
		return errors.NewConnectionError(r.options.URI,
			fmt.Errorf("ping failed: %w", err))
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
		return errors.ErrNotConnected
	}

	conn := r.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("PING"); err != nil {
		return errors.NewConnectionError(r.options.URI,
			fmt.Errorf("health check failed: %w", err))
	}

	return nil
}

// Type returns the broker type
func (r *RedisBroker) Type() string {
	return "redis"
}

// SetLogger sets the logger for the broker
func (r *RedisBroker) SetLogger(logger seelog.LoggerInterface) {
	r.logger = logger
}

// Capabilities returns Redis broker capabilities
func (r *RedisBroker) Capabilities() core.BrokerCapabilities {
	return core.BrokerCapabilities{
		SupportsAck:        false, // Redis doesn't have built-in ACK
		SupportsDelay:      false, // Could be implemented with sorted sets
		SupportsPriority:   false, // Could be implemented with multiple queues
		SupportsDeadLetter: false, // Could be implemented
	}
}

// Enqueue adds a job to the queue
func (r *RedisBroker) Enqueue(ctx context.Context, j job.Job) error {
	if r.pool == nil {
		return errors.ErrNotConnected
	}

	conn := r.pool.Get()
	defer conn.Close()

	// Serialize job
	data, err := r.serializer.Serialize(j)
	if err != nil {
		return errors.NewSerializationError(r.serializer.GetFormat(),
			fmt.Errorf("serialize job: %w", err))
	}

	queueKey := r.queueKey(j.GetQueue())

	// Add to queue
	if _, err := conn.Do("RPUSH", queueKey, data); err != nil {
		return errors.NewBrokerError("enqueue", j.GetQueue(), err)
	}

	// Add queue to set of known queues (best effort)
	if _, err := conn.Do("SADD", r.queuesKey(), j.GetQueue()); err != nil {
		r.logError("Failed to track queue %s: %v", j.GetQueue(), err)
	}

	return nil
}

// Dequeue retrieves a job from the queue
func (r *RedisBroker) Dequeue(ctx context.Context, queue string) (job.Job, error) {
	if r.pool == nil {
		return nil, errors.ErrNotConnected
	}

	conn := r.pool.Get()
	defer conn.Close()

	queueKey := r.queueKey(queue)

	// Pop from queue
	reply, err := conn.Do("LPOP", queueKey)
	if err != nil {
		return nil, errors.NewBrokerError("dequeue", queue, err)
	}

	if reply == nil {
		return nil, nil // No job available
	}

	data, ok := reply.([]byte)
	if !ok {
		return nil, errors.NewBrokerError("dequeue", queue,
			fmt.Errorf("unexpected data type: %T", reply))
	}

	// Create metadata
	metadata := job.Metadata{
		Queue:      queue,
		EnqueuedAt: time.Now(), // Redis doesn't store this
	}

	// Deserialize job
	j, err := r.serializer.Deserialize(data, metadata)
	if err != nil {
		return nil, errors.NewSerializationError(r.serializer.GetFormat(),
			fmt.Errorf("deserialize job: %w", err))
	}

	return j, nil
}

// Ack acknowledges job completion (no-op for Redis)
func (r *RedisBroker) Ack(ctx context.Context, j job.Job) error {
	// Redis doesn't have built-in ACK mechanism
	// Job is already removed from queue by LPOP
	return nil
}

// Nack rejects a job and optionally requeues it
func (r *RedisBroker) Nack(ctx context.Context, j job.Job, requeue bool) error {
	if !requeue {
		return nil // Nothing to do
	}

	// Requeue the job
	return r.Enqueue(ctx, j)
}

// CreateQueue creates a new queue (no-op for Redis)
func (r *RedisBroker) CreateQueue(ctx context.Context, name string, options core.QueueOptions) error {
	// Redis queues are created on-demand
	return nil
}

// DeleteQueue deletes a queue
func (r *RedisBroker) DeleteQueue(ctx context.Context, name string) error {
	if r.pool == nil {
		return errors.ErrNotConnected
	}

	conn := r.pool.Get()
	defer conn.Close()

	queueKey := r.queueKey(name)

	// Delete the queue
	if _, err := conn.Do("DEL", queueKey); err != nil {
		return errors.NewBrokerError("delete_queue", name, err)
	}

	// Remove from set of queues (best effort)
	if _, err := conn.Do("SREM", r.queuesKey(), name); err != nil {
		r.logError("Failed to remove queue %s from set: %v", name, err)
	}

	return nil
}

// QueueExists checks if a queue exists
func (r *RedisBroker) QueueExists(ctx context.Context, name string) (bool, error) {
	if r.pool == nil {
		return false, errors.ErrNotConnected
	}

	conn := r.pool.Get()
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("SISMEMBER", r.queuesKey(), name))
	if err != nil {
		return false, errors.NewBrokerError("queue_exists", name, err)
	}

	return exists, nil
}

// QueueLength returns the number of jobs in a queue
func (r *RedisBroker) QueueLength(ctx context.Context, name string) (int64, error) {
	if r.pool == nil {
		return 0, errors.ErrNotConnected
	}

	conn := r.pool.Get()
	defer conn.Close()

	queueKey := r.queueKey(name)

	length, err := redis.Int64(conn.Do("LLEN", queueKey))
	if err != nil {
		return 0, errors.NewBrokerError("queue_length", name, err)
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

func (r *RedisBroker) logError(format string, args ...interface{}) {
	if r.logger != nil {
		r.logger.Errorf(format, args...)
	}
}
