package resque

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/BranchIntl/goworker2/core"
	"github.com/BranchIntl/goworker2/errors"
	redisUtils "github.com/BranchIntl/goworker2/internal/redis"
	"github.com/BranchIntl/goworker2/job"
	"github.com/gomodule/redigo/redis"
)

// ResqueStatistics implements the Statistics interface for Resque
type ResqueStatistics struct {
	pool      *redis.Pool
	namespace string
	options   Options
}

// NewStatistics creates a new Resque statistics backend
func NewStatistics(options Options) *ResqueStatistics {
	return &ResqueStatistics{
		namespace: options.Namespace,
		options:   options,
	}
}

// Connect establishes connection to Redis
func (r *ResqueStatistics) Connect(ctx context.Context) error {
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
func (r *ResqueStatistics) Close() error {
	if r.pool != nil {
		return r.pool.Close()
	}
	return nil
}

// Health checks the Redis connection health
func (r *ResqueStatistics) Health() error {
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

// Type returns the statistics backend type
func (r *ResqueStatistics) Type() string {
	return "resque"
}

// RegisterWorker registers a worker in Redis
func (r *ResqueStatistics) RegisterWorker(ctx context.Context, worker core.WorkerInfo) error {
	conn := r.pool.Get()
	defer conn.Close()

	workerKey := r.workerKey(worker.ID)
	workersKey := r.workersKey()

	// Add to workers set
	if _, err := conn.Do("SADD", workersKey, worker.ID); err != nil {
		return fmt.Errorf("failed to add worker to set: %w", err)
	}

	// Set worker info
	workerData, err := json.Marshal(worker)
	if err != nil {
		return fmt.Errorf("failed to marshal worker info: %w", err)
	}

	if _, err := conn.Do("SET", workerKey, workerData); err != nil {
		return fmt.Errorf("failed to set worker info: %w", err)
	}

	// Initialize stats
	if _, err := conn.Do("SET", r.statProcessedKey(worker.ID), "0"); err != nil {
		return fmt.Errorf("failed to initialize processed stat: %w", err)
	}

	if _, err := conn.Do("SET", r.statFailedKey(worker.ID), "0"); err != nil {
		return fmt.Errorf("failed to initialize failed stat: %w", err)
	}

	if _, err := conn.Do("SET", r.workerStartedKey(worker.ID), worker.Started.Format(time.RFC3339)); err != nil {
		return fmt.Errorf("failed to set worker started time: %w", err)
	}

	return nil
}

// UnregisterWorker removes a worker from Redis
func (r *ResqueStatistics) UnregisterWorker(ctx context.Context, workerID string) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Remove from workers set
	if _, err := conn.Do("SREM", r.workersKey(), workerID); err != nil {
		return fmt.Errorf("failed to remove worker from set: %w", err)
	}

	// Delete worker keys
	keys := []string{
		r.workerKey(workerID),
		r.statProcessedKey(workerID),
		r.statFailedKey(workerID),
		r.workerStartedKey(workerID),
	}

	for _, key := range keys {
		if _, err := conn.Do("DEL", key); err != nil {
			return fmt.Errorf("failed to delete key %s: %w", key, err)
		}
	}

	return nil
}

// RecordJobStarted records that a job has started
func (r *ResqueStatistics) RecordJobStarted(ctx context.Context, job job.Job, worker core.WorkerInfo) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Store job info for worker
	workData := map[string]interface{}{
		"queue":   job.GetQueue(),
		"run_at":  time.Now().Format(time.RFC3339),
		"payload": job.GetPayload(),
	}

	workJSON, err := json.Marshal(workData)
	if err != nil {
		return fmt.Errorf("failed to marshal work data: %w", err)
	}

	if _, err := conn.Do("SET", r.workerJobKey(worker.ID), workJSON); err != nil {
		return fmt.Errorf("failed to set worker job: %w", err)
	}

	return nil
}

// RecordJobCompleted records successful job completion
func (r *ResqueStatistics) RecordJobCompleted(ctx context.Context, job job.Job, worker core.WorkerInfo, duration time.Duration) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Increment counters
	if _, err := conn.Do("INCR", r.statProcessedKey("")); err != nil {
		return fmt.Errorf("failed to increment global processed: %w", err)
	}

	if _, err := conn.Do("INCR", r.statProcessedKey(worker.ID)); err != nil {
		return fmt.Errorf("failed to increment worker processed: %w", err)
	}

	// Clear worker job
	if _, err := conn.Do("DEL", r.workerJobKey(worker.ID)); err != nil {
		return fmt.Errorf("failed to clear worker job: %w", err)
	}

	return nil
}

// RecordJobFailed records job failure
func (r *ResqueStatistics) RecordJobFailed(ctx context.Context, job job.Job, worker core.WorkerInfo, err error, duration time.Duration) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Store failure details with rich worker information
	failure := map[string]interface{}{
		"failed_at": time.Now().Format(time.RFC3339),
		"payload":   job.GetPayload(),
		"exception": "Error",
		"error":     err.Error(),
		"worker":    worker,
		"queue":     job.GetQueue(),
	}

	failureJSON, jsonErr := json.Marshal(failure)
	if jsonErr != nil {
		return fmt.Errorf("failed to marshal failure data: %w", jsonErr)
	}

	// Store in failed queue (like original Resque)
	if _, err := conn.Do("RPUSH", r.failedKey(), failureJSON); err != nil {
		return fmt.Errorf("failed to store failure: %w", err)
	}

	// Increment counters
	if _, err := conn.Do("INCR", r.statFailedKey("")); err != nil {
		return fmt.Errorf("failed to increment global failed: %w", err)
	}

	if _, err := conn.Do("INCR", r.statFailedKey(worker.ID)); err != nil {
		return fmt.Errorf("failed to increment worker failed: %w", err)
	}

	// Clear worker job
	if _, err := conn.Do("DEL", r.workerJobKey(worker.ID)); err != nil {
		return fmt.Errorf("failed to clear worker job: %w", err)
	}

	return nil
}

// GetWorkerStats returns statistics for a specific worker
func (r *ResqueStatistics) GetWorkerStats(ctx context.Context, workerID string) (core.WorkerStats, error) {
	conn := r.pool.Get()
	defer conn.Close()

	// Get processed count
	processed, err := redis.Int64(conn.Do("GET", r.statProcessedKey(workerID)))
	if err != nil && err != redis.ErrNil {
		return core.WorkerStats{}, fmt.Errorf("failed to get processed count: %w", err)
	}

	// Get failed count
	failed, err := redis.Int64(conn.Do("GET", r.statFailedKey(workerID)))
	if err != nil && err != redis.ErrNil {
		return core.WorkerStats{}, fmt.Errorf("failed to get failed count: %w", err)
	}

	// Get start time
	startTimeStr, err := redis.String(conn.Do("GET", r.workerStartedKey(workerID)))
	var startTime time.Time
	if err == nil {
		startTime, _ = time.Parse(time.RFC3339, startTimeStr)
	}

	// Check if worker has current job
	inProgress := int64(0)
	exists, err := redis.Bool(conn.Do("EXISTS", r.workerJobKey(workerID)))
	if err == nil && exists {
		inProgress = 1
	}

	return core.WorkerStats{
		ID:         workerID,
		Processed:  processed,
		Failed:     failed,
		InProgress: inProgress,
		StartTime:  startTime,
		LastJob:    time.Now(), // Could track this better
	}, nil
}

// GetQueueStats returns statistics for a queue
func (r *ResqueStatistics) GetQueueStats(ctx context.Context, queue string) (core.QueueStats, error) {
	conn := r.pool.Get()
	defer conn.Close()

	queueKey := r.queueKey(queue)

	// Get queue length
	length, err := redis.Int64(conn.Do("LLEN", queueKey))
	if err != nil {
		return core.QueueStats{}, fmt.Errorf("failed to get queue length: %w", err)
	}

	return core.QueueStats{
		Name:      queue,
		Length:    length,
		Processed: 0, // Could track this per queue
		Failed:    0, // Could track this per queue
		Workers:   0, // Could track active workers per queue
	}, nil
}

// GetGlobalStats returns global statistics
func (r *ResqueStatistics) GetGlobalStats(ctx context.Context) (core.GlobalStats, error) {
	conn := r.pool.Get()
	defer conn.Close()

	// Get global processed count
	processed, err := redis.Int64(conn.Do("GET", r.statProcessedKey("")))
	if err != nil && err != redis.ErrNil {
		return core.GlobalStats{}, fmt.Errorf("failed to get global processed: %w", err)
	}

	// Get global failed count
	failed, err := redis.Int64(conn.Do("GET", r.statFailedKey("")))
	if err != nil && err != redis.ErrNil {
		return core.GlobalStats{}, fmt.Errorf("failed to get global failed: %w", err)
	}

	// Get active workers count
	activeWorkers, err := redis.Int64(conn.Do("SCARD", r.workersKey()))
	if err != nil {
		return core.GlobalStats{}, fmt.Errorf("failed to get active workers: %w", err)
	}

	// For now, return empty queue stats map
	// Could be enhanced to get stats for all known queues
	queueStats := make(map[string]core.QueueStats)

	return core.GlobalStats{
		TotalProcessed: processed,
		TotalFailed:    failed,
		ActiveWorkers:  activeWorkers,
		QueueStats:     queueStats,
	}, nil
}

// Helper methods for Redis keys

func (r *ResqueStatistics) workerKey(workerID string) string {
	return fmt.Sprintf("%sworker:%s", r.namespace, workerID)
}

func (r *ResqueStatistics) workersKey() string {
	return fmt.Sprintf("%sworkers", r.namespace)
}

func (r *ResqueStatistics) statProcessedKey(workerID string) string {
	if workerID == "" {
		return fmt.Sprintf("%sstat:processed", r.namespace)
	}
	return fmt.Sprintf("%sstat:processed:%s", r.namespace, workerID)
}

func (r *ResqueStatistics) statFailedKey(workerID string) string {
	if workerID == "" {
		return fmt.Sprintf("%sstat:failed", r.namespace)
	}
	return fmt.Sprintf("%sstat:failed:%s", r.namespace, workerID)
}

func (r *ResqueStatistics) workerStartedKey(workerID string) string {
	return fmt.Sprintf("%sworker:%s:started", r.namespace, workerID)
}

func (r *ResqueStatistics) workerJobKey(workerID string) string {
	return fmt.Sprintf("%sworker:%s:job", r.namespace, workerID)
}

func (r *ResqueStatistics) failedKey() string {
	return fmt.Sprintf("%sfailed", r.namespace)
}

func (r *ResqueStatistics) queueKey(queue string) string {
	return fmt.Sprintf("%squeue:%s", r.namespace, queue)
}
