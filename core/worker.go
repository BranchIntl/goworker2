package core

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
)

// Worker represents an individual worker
type Worker struct {
	id       string
	hostname string
	pid      int
	registry Registry
	stats    Statistics
	broker   Broker

	// Statistics
	processed int64
	failed    int64
	startTime time.Time
}

// NewWorker creates a new worker
func NewWorker(
	id string,
	registry Registry,
	stats Statistics,
	broker Broker,
) *Worker {
	hostname, _ := os.Hostname()

	return &Worker{
		id:        id,
		hostname:  hostname,
		pid:       os.Getpid(),
		registry:  registry,
		stats:     stats,
		broker:    broker,
		startTime: time.Now(),
	}
}

// GetID returns the worker's unique ID
func (w *Worker) GetID() string {
	return fmt.Sprintf("%s:%d-%s", w.hostname, w.pid, w.id)
}

// Work starts processing jobs
func (w *Worker) Work(ctx context.Context, jobs <-chan job.Job) error {
	// Register worker
	workerInfo := WorkerInfo{
		ID:       w.GetID(),
		Hostname: w.hostname,
		Pid:      w.pid,
		Queues:   w.broker.Queues(),
		Started:  w.startTime,
	}

	if err := w.stats.RegisterWorker(ctx, workerInfo); err != nil {
		slog.Error("Failed to register worker", "error", err)
	}

	defer func() {
		if err := w.stats.UnregisterWorker(ctx, w.GetID()); err != nil {
			slog.Error("Failed to unregister worker", "error", err)
		}
	}()

	slog.Info("Worker started", "id", w.GetID())

	for {
		select {
		case <-ctx.Done():
			slog.Info("Worker stopping", "id", w.GetID())
			return nil
		case job, ok := <-jobs:
			if !ok {
				slog.Info("Worker job channel closed", "id", w.GetID())
				return nil
			}

			w.processJob(ctx, job)
		}
	}
}

// processJob handles a single job
func (w *Worker) processJob(ctx context.Context, job job.Job) {
	startTime := time.Now()
	workerInfo := WorkerInfo{
		ID:       w.GetID(),
		Hostname: w.hostname,
		Pid:      w.pid,
		Queues:   w.broker.Queues(),
		Started:  w.startTime,
	}

	// Record job started
	if err := w.stats.RecordJobStarted(ctx, job, workerInfo); err != nil {
		slog.Error("Failed to record job start", "error", err)
	}

	// Get worker function
	workerFunc, ok := w.registry.Get(job.GetClass())
	if !ok {
		err := errors.NewWorkerError(job.GetClass(), job.GetQueue(), errors.ErrWorkerNotFound)
		w.handleJobError(ctx, job, workerInfo, err, startTime)
		// Nack the job for unknown worker
		if err := w.broker.Nack(ctx, job, false); err != nil {
			slog.Error("Failed to nack job", "error", err)
		}
		return
	}

	// Execute job with panic recovery
	err := w.executeJob(workerFunc, job)

	if err != nil {
		w.handleJobError(ctx, job, workerInfo, err, startTime)
		// Nack the job on error
		if err := w.broker.Nack(ctx, job, false); err != nil {
			slog.Error("Failed to nack job", "error", err)
		}
	} else {
		w.handleJobSuccess(ctx, job, workerInfo, startTime)
		// Ack the job on success
		if err := w.broker.Ack(ctx, job); err != nil {
			slog.Error("Failed to ack job", "error", err)
		}
	}
}

// executeJob runs the worker function with panic recovery
func (w *Worker) executeJob(workerFunc WorkerFunc, job job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewWorkerError(job.GetClass(), job.GetQueue(),
				fmt.Errorf("panic: %v", r))
		}
	}()

	if execErr := workerFunc(job.GetQueue(), job.GetArgs()...); execErr != nil {
		return errors.NewWorkerError(job.GetClass(), job.GetQueue(), execErr)
	}

	return nil
}

// handleJobSuccess records successful job completion
func (w *Worker) handleJobSuccess(ctx context.Context, job job.Job, worker WorkerInfo, startTime time.Time) {
	duration := time.Since(startTime)

	atomic.AddInt64(&w.processed, 1)

	if err := w.stats.RecordJobCompleted(ctx, job, worker, duration); err != nil {
		slog.Error("Failed to record job completion", "error", err)
	}

	slog.Debug("Job completed", "class", job.GetClass(), "duration", duration)
}

// handleJobError records job failure
func (w *Worker) handleJobError(ctx context.Context, job job.Job, worker WorkerInfo, err error, startTime time.Time) {
	duration := time.Since(startTime)

	atomic.AddInt64(&w.failed, 1)

	if err := w.stats.RecordJobFailed(ctx, job, worker, err, duration); err != nil {
		slog.Error("Failed to record job failure", "error", err)
	}

	slog.Error("Job failed", "class", job.GetClass(), "error", err)
}

// GetStats returns current worker statistics
func (w *Worker) GetStats() WorkerStats {
	return WorkerStats{
		ID:         w.GetID(),
		Processed:  atomic.LoadInt64(&w.processed),
		Failed:     atomic.LoadInt64(&w.failed),
		InProgress: 0, // Could track this with atomic counter
		StartTime:  w.startTime,
		LastJob:    time.Now(), // Could track this properly
	}
}
