package core

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/BranchIntl/goworker2/job"
)

// WorkerPool manages a pool of workers
type WorkerPool struct {
	registry      Registry
	stats         Statistics
	serializer    Serializer
	concurrency   int
	queues        []string
	jobChan       <-chan job.Job
	activeWorkers int32
	workers       []*Worker
	wg            sync.WaitGroup
	broker        Broker
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(
	registry Registry,
	stats Statistics,
	serializer Serializer,
	concurrency int,
	queues []string,
	jobChan <-chan job.Job,
	broker Broker,
) *WorkerPool {
	return &WorkerPool{
		registry:    registry,
		stats:       stats,
		serializer:  serializer,
		concurrency: concurrency,
		queues:      queues,
		jobChan:     jobChan,
		workers:     make([]*Worker, 0, concurrency),
		broker:      broker,
	}
}

// Start begins processing jobs with the worker pool
func (wp *WorkerPool) Start(ctx context.Context) error {
	slog.Info("Starting worker pool", "workers", wp.concurrency)

	// Create workers
	for i := 0; i < wp.concurrency; i++ {
		worker := NewWorker(
			strconv.Itoa(i),
			wp.queues,
			wp.registry,
			wp.stats,
			wp.broker,
		)
		wp.workers = append(wp.workers, worker)
	}

	// Start workers
	for _, worker := range wp.workers {
		wp.wg.Add(1)
		go func(w *Worker) {
			defer wp.wg.Done()
			atomic.AddInt32(&wp.activeWorkers, 1)
			defer atomic.AddInt32(&wp.activeWorkers, -1)

			if err := w.Work(ctx, wp.jobChan); err != nil {
				slog.Error("Worker error", "error", err)
			}
		}(worker)
	}

	// Wait for all workers to complete
	wp.wg.Wait()
	slog.Info("Worker pool stopped")
	return nil
}

// ActiveWorkers returns the number of active workers
func (wp *WorkerPool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&wp.activeWorkers))
}

// GetWorkerStats returns statistics for all workers
func (wp *WorkerPool) GetWorkerStats() []WorkerStats {
	stats := make([]WorkerStats, 0, len(wp.workers))
	for _, worker := range wp.workers {
		stats = append(stats, worker.GetStats())
	}
	return stats
}
