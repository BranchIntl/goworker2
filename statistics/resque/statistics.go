package resque

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/benmanns/goworker/interfaces"
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
func (r *ResqueStatistics) Close() error {
	if r.pool != nil {
		return r.pool.Close()
	}
	return nil
}

// Health checks the Redis connection health
func (r *ResqueStatistics) Health() error {
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

// Type returns the statistics backend type
func (r *ResqueStatistics) Type() string {
	return "resque"
}

// RegisterWorker registers a worker in Redis
func (r *ResqueStatistics) RegisterWorker(ctx context.Context, worker interfaces.WorkerInfo) error {
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
func (r *ResqueStatistics) RecordJobStarted(ctx context.Context, job interfaces.JobInfo) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Store job info for worker
	workData := map[string]interface{}{
		"queue":  job.Queue,
		"run_at": time.Now().Format(time.RFC3339),
		"payload": map[string]interface{}{
			"class": job.Class,
		},
	}

	workJSON, err := json.Marshal(workData)
	if err != nil {
		return fmt.Errorf("failed to marshal work data: %w", err)
	}

	if _, err := conn.Do("SET", r.workerJobKey(job.WorkerID), workJSON); err != nil {
		return fmt.Errorf("failed to set worker job: %w", err)
	}

	return nil
}

// RecordJobCompleted records successful job completion
func (r *ResqueStatistics) RecordJobCompleted(ctx context.Context, job interfaces.JobInfo, duration time.Duration) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Increment counters
	if _, err := conn.Do("INCR", r.statProcessedKey("")); err != nil {
		return fmt.Errorf("failed to increment global processed: %w", err)
	}

	if _, err := conn.Do("INCR", r.statProcessedKey(job.WorkerID)); err != nil {
		return fmt.Errorf("failed to increment worker processed: %w", err)
	}

	// Clear worker job
	if _, err := conn.Do("DEL", r.workerJobKey(job.WorkerID)); err != nil {
		return fmt.Errorf("failed to clear worker job: %w", err)
	}

	return nil
}

// RecordJobFailed records job failure
func (r *ResqueStatistics) RecordJobFailed(ctx context.Context, job interfaces.JobInfo, err error, duration time.Duration) error {
	conn := r.pool.Get()
	defer conn.Close()

	// Increment counters
	if _, err := conn.Do("INCR", r.statFailedKey("")); err != nil {
		return fmt.Errorf("failed to increment global failed: %w", err)
	}

	if _, err := conn.Do("INCR", r.statFailedKey(job.WorkerID)); err != nil {
		return fmt.Errorf("failed to increment worker failed: %w", err)
	}

	// Store failure info
	failure := map[string]interface{}{
		"failed_at": time.Now().Format(time.RFC3339),
		"queue":     job.Queue,
		"class":     job.Class,
		"error":     err.Error(),
		"worker":    job.WorkerID,
	}

	failureJSON, err := json.Marshal(failure)
	if err != nil {
		return fmt.Errorf("failed to marshal failure: %w", err)
	}

	if _, err := conn.Do("RPUSH", r.failedKey(), failureJSON); err != nil {
		return fmt.Errorf("failed to push failure: %w", err)
	}

	// Clear worker job
	if _, err := conn.Do("DEL", r.workerJobKey(job.WorkerID)); err != nil {
		return fmt.Errorf("failed to clear worker job: %w", err)
	}

	return nil
}

// GetWorkerStats returns statistics for a specific worker
func (r *ResqueStatistics) GetWorkerStats(ctx context.Context, workerID string) (interfaces.WorkerStats, error) {
	conn := r.pool.Get()
	defer conn.Close()

	processed, err := redis.Int64(conn.Do("GET", r.statProcessedKey(workerID)))
	if err != nil && err != redis.ErrNil {
		return interfaces.WorkerStats{}, fmt.Errorf("failed to get processed count: %w", err)
	}

	failed, err := redis.Int64(conn.Do("GET", r.statFailedKey(workerID)))
	if err != nil && err != redis.ErrNil {
		return interfaces.WorkerStats{}, fmt.Errorf("failed to get failed count: %w", err)
	}

	startedStr, err := redis.String(conn.Do("GET", r.workerStartedKey(workerID)))
	if err != nil && err != redis.ErrNil {
		return interfaces.WorkerStats{}, fmt.Errorf("failed to get started time: %w", err)
	}

	var startTime time.Time
	if startedStr != "" {
		startTime, _ = time.Parse(time.RFC3339, startedStr)
	}

	return interfaces.WorkerStats{
		ID:         workerID,
		Processed:  processed,
		Failed:     failed,
		InProgress: 0, // Could check if worker has active job
		StartTime:  startTime,
		LastJob:    time.Now(), // Could track this
	}, nil
}

// GetQueueStats returns statistics for a specific queue
func (r *ResqueStatistics) GetQueueStats(ctx context.Context, queue string) (interfaces.QueueStats, error) {
	conn := r.pool.Get()
	defer conn.Close()

	// Get queue length
	length, err := redis.Int64(conn.Do("LLEN", r.queueKey(queue)))
	if err != nil {
		return interfaces.QueueStats{}, fmt.Errorf("failed to get queue length: %w", err)
	}

	// Count workers processing this queue
	// This would require tracking which workers are assigned to which queues

	return interfaces.QueueStats{
		Name:      queue,
		Length:    length,
		Processed: 0, // Would need per-queue counters
		Failed:    0, // Would need per-queue counters
		Workers:   0, // Would need to track
	}, nil
}

// GetGlobalStats returns global statistics
func (r *ResqueStatistics) GetGlobalStats(ctx context.Context) (interfaces.GlobalStats, error) {
	conn := r.pool.Get()
	defer conn.Close()

	processed, err := redis.Int64(conn.Do("GET", r.statProcessedKey("")))
	if err != nil && err != redis.ErrNil {
		return interfaces.GlobalStats{}, fmt.Errorf("failed to get global processed: %w", err)
	}

	failed, err := redis.Int64(conn.Do("GET", r.statFailedKey("")))
	if err != nil && err != redis.ErrNil {
		return interfaces.GlobalStats{}, fmt.Errorf("failed to get global failed: %w", err)
	}

	// Count active workers
	workers, err := redis.Strings(conn.Do("SMEMBERS", r.workersKey()))
	if err != nil {
		return interfaces.GlobalStats{}, fmt.Errorf("failed to get workers: %w", err)
	}

	return interfaces.GlobalStats{
		TotalProcessed: processed,
		TotalFailed:    failed,
		ActiveWorkers:  int64(len(workers)),
		QueueStats:     make(map[string]interfaces.QueueStats), // Would need to populate
	}, nil
}

// Helper methods for key generation

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
	return fmt.Sprintf("%sworker:%s", r.namespace, workerID)
}

func (r *ResqueStatistics) failedKey() string {
	return fmt.Sprintf("%sfailed", r.namespace)
}

func (r *ResqueStatistics) queueKey(queue string) string {
	return fmt.Sprintf("%squeue:%s", r.namespace, queue)
}
