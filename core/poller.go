package core

import (
	"context"
	"log/slog"
	"time"

	"github.com/BranchIntl/goworker2/job"
)

// StandardPoller is a broker-agnostic job poller for pull-based brokers
type StandardPoller struct {
	broker   Broker
	stats    Statistics
	queues   []string
	interval time.Duration
}

// NewStandardPoller creates a new standard poller
func NewStandardPoller(
	broker Broker,
	stats Statistics,
	queues []string,
	interval time.Duration,
) *StandardPoller {
	return &StandardPoller{
		broker:   broker,
		stats:    stats,
		queues:   queues,
		interval: interval,
	}
}

// Start begins polling for jobs
func (p *StandardPoller) Start(ctx context.Context, jobChan chan<- job.Job) error {
	slog.Info("StandardPoller started", "queues", p.queues)

	for {
		select {
		case <-ctx.Done():
			close(jobChan)
			slog.Info("StandardPoller stopped")
			return nil
		default:
			job, err := p.pollOnce(ctx)
			if err != nil {
				slog.Error("Error polling", "error", err)
				// Brief pause on error
				time.Sleep(time.Second)
				continue
			}

			if job != nil {
				select {
				case <-ctx.Done():
					// Put job back on queue
					if err := p.broker.Nack(ctx, job, true); err != nil {
						slog.Error("Error requeueing job", "error", err)
					}
					close(jobChan)
					slog.Info("StandardPoller stopped")
					return nil
				case jobChan <- job:
					slog.Debug("Job sent to workers", "class", job.GetClass())
				}
			} else {
				// No job found, wait before polling again
				select {
				case <-ctx.Done():
					close(jobChan)
					slog.Info("StandardPoller stopped")
					return nil
				case <-time.After(p.interval):
				}
			}
		}
	}
}

// pollOnce attempts to get a job from the queues
func (p *StandardPoller) pollOnce(ctx context.Context) (job.Job, error) {
	queues := p.queues

	for _, queue := range queues {
		slog.Debug("Checking queue", "queue", queue)

		job, err := p.broker.Dequeue(ctx, queue)
		if err != nil {
			return nil, err
		}

		if job != nil {
			slog.Debug("Found job", "queue", queue, "class", job.GetClass())
			return job, nil
		}
	}

	return nil, nil
}
