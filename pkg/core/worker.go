package core

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/cihub/seelog"
)

// Worker represents an individual worker
type Worker struct {
	id       string
	hostname string
	pid      int
	registry interfaces.Registry
	stats    interfaces.Statistics
	logger   seelog.LoggerInterface

	// Statistics
	processed int64
	failed    int64
	startTime time.Time
}

// NewWorker creates a new worker
func NewWorker(
	id string,
	registry interfaces.Registry,
	stats interfaces.Statistics,
	logger seelog.LoggerInterface,
) *Worker {
	hostname, _ := os.Hostname()

	return &Worker{
		id:        id,
		hostname:  hostname,
		pid:       os.Getpid(),
		registry:  registry,
		stats:     stats,
		logger:    logger,
		startTime: time.Now(),
	}
}

// GetID returns the worker's unique ID
func (w *Worker) GetID() string {
	return fmt.Sprintf("%s:%d-%s", w.hostname, w.pid, w.id)
}

// GetQueues returns the queues this worker processes
func (w *Worker) GetQueues() []string {
	// In this implementation, queues are managed by the poller
	// Workers process jobs from any queue
	return []string{}
}

// Work starts processing jobs
func (w *Worker) Work(ctx context.Context, jobs <-chan interfaces.Job) error {
	// Register worker
	workerInfo := interfaces.WorkerInfo{
		ID:       w.GetID(),
		Hostname: w.hostname,
		Pid:      w.pid,
		Queues:   w.GetQueues(),
		Started:  w.startTime,
	}

	if err := w.stats.RegisterWorker(ctx, workerInfo); err != nil {
		w.logger.Errorf("Failed to register worker: %v", err)
	}

	defer func() {
		if err := w.stats.UnregisterWorker(ctx, w.GetID()); err != nil {
			w.logger.Errorf("Failed to unregister worker: %v", err)
		}
	}()

	w.logger.Infof("Worker %s started", w.GetID())

	for {
		select {
		case <-ctx.Done():
			w.logger.Infof("Worker %s stopping", w.GetID())
			return nil
		case job, ok := <-jobs:
			if !ok {
				w.logger.Infof("Worker %s job channel closed", w.GetID())
				return nil
			}

			w.processJob(ctx, job)
		}
	}
}

// processJob handles a single job
func (w *Worker) processJob(ctx context.Context, job interfaces.Job) {
	startTime := time.Now()
	jobInfo := interfaces.JobInfo{
		ID:       job.GetID(),
		Queue:    job.GetQueue(),
		Class:    job.GetClass(),
		WorkerID: w.GetID(),
	}

	// Record job started
	if err := w.stats.RecordJobStarted(ctx, jobInfo); err != nil {
		w.logger.Errorf("Failed to record job start: %v", err)
	}

	// Get worker function
	workerFunc, ok := w.registry.Get(job.GetClass())
	if !ok {
		err := fmt.Errorf("no worker registered for class: %s", job.GetClass())
		w.handleJobError(ctx, job, jobInfo, err, startTime)
		return
	}

	// Execute job with panic recovery
	err := w.executeJob(workerFunc, job)

	if err != nil {
		w.handleJobError(ctx, job, jobInfo, err, startTime)
	} else {
		w.handleJobSuccess(ctx, jobInfo, startTime)
	}
}

// executeJob runs the worker function with panic recovery
func (w *Worker) executeJob(workerFunc interfaces.WorkerFunc, job interfaces.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	return workerFunc(job.GetQueue(), job.GetArgs()...)
}

// handleJobSuccess records successful job completion
func (w *Worker) handleJobSuccess(ctx context.Context, jobInfo interfaces.JobInfo, startTime time.Time) {
	duration := time.Since(startTime)

	atomic.AddInt64(&w.processed, 1)

	if err := w.stats.RecordJobCompleted(ctx, jobInfo, duration); err != nil {
		w.logger.Errorf("Failed to record job completion: %v", err)
	}

	w.logger.Debugf("Job %s completed in %v", jobInfo.Class, duration)
}

// handleJobError records job failure
func (w *Worker) handleJobError(ctx context.Context, job interfaces.Job, jobInfo interfaces.JobInfo, err error, startTime time.Time) {
	duration := time.Since(startTime)

	atomic.AddInt64(&w.failed, 1)

	if err := w.stats.RecordJobFailed(ctx, jobInfo, err, duration); err != nil {
		w.logger.Errorf("Failed to record job failure: %v", err)
	}

	w.logger.Errorf("Job %s failed: %v", jobInfo.Class, err)
}

// GetStats returns current worker statistics
func (w *Worker) GetStats() interfaces.WorkerStats {
	return interfaces.WorkerStats{
		ID:         w.GetID(),
		Processed:  atomic.LoadInt64(&w.processed),
		Failed:     atomic.LoadInt64(&w.failed),
		InProgress: 0, // Could track this with atomic counter
		StartTime:  w.startTime,
		LastJob:    time.Now(), // Could track this properly
	}
}
