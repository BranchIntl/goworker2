package core

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/cihub/seelog"
)

// WorkerPool manages a pool of workers
type WorkerPool struct {
	registry      interfaces.Registry
	stats         interfaces.Statistics
	serializer    interfaces.Serializer
	concurrency   int
	jobChan       <-chan interfaces.Job
	logger        seelog.LoggerInterface
	activeWorkers int32
	workers       []*Worker
	wg            sync.WaitGroup
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(
	registry interfaces.Registry,
	stats interfaces.Statistics,
	serializer interfaces.Serializer,
	concurrency int,
	jobChan <-chan interfaces.Job,
	logger seelog.LoggerInterface,
) *WorkerPool {
	return &WorkerPool{
		registry:    registry,
		stats:       stats,
		serializer:  serializer,
		concurrency: concurrency,
		jobChan:     jobChan,
		logger:      logger,
		workers:     make([]*Worker, 0, concurrency),
	}
}

// Start begins processing jobs with the worker pool
func (wp *WorkerPool) Start(ctx context.Context) error {
	wp.logger.Infof("Starting worker pool with %d workers", wp.concurrency)

	// Create workers
	for i := 0; i < wp.concurrency; i++ {
		worker := NewWorker(
			strconv.Itoa(i),
			wp.registry,
			wp.stats,
			wp.logger,
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
				wp.logger.Errorf("Worker error: %v", err)
			}
		}(worker)
	}

	// Wait for all workers to complete
	wp.wg.Wait()
	wp.logger.Info("Worker pool stopped")
	return nil
}

// ActiveWorkers returns the number of active workers
func (wp *WorkerPool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&wp.activeWorkers))
}

// GetWorkerStats returns statistics for all workers
func (wp *WorkerPool) GetWorkerStats() []interfaces.WorkerStats {
	stats := make([]interfaces.WorkerStats, 0, len(wp.workers))
	for _, worker := range wp.workers {
		stats = append(stats, worker.GetStats())
	}
	return stats
}
