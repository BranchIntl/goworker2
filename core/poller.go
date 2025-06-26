package core

import (
	"context"
	"math/rand"
	"time"

	"github.com/BranchIntl/goworker2/job"
	"github.com/cihub/seelog"
)

// StandardPoller is a broker-agnostic job poller for pull-based brokers
type StandardPoller struct {
	broker      Broker
	stats       Statistics
	queues      []string
	interval    time.Duration
	logger      seelog.LoggerInterface
	strictOrder bool
}

// NewStandardPoller creates a new standard poller
func NewStandardPoller(
	broker Broker,
	stats Statistics,
	queues []string,
	interval time.Duration,
	logger seelog.LoggerInterface,
) *StandardPoller {
	return &StandardPoller{
		broker:      broker,
		stats:       stats,
		queues:      queues,
		interval:    interval,
		logger:      logger,
		strictOrder: true,
	}
}

// Start begins polling for jobs
func (p *StandardPoller) Start(ctx context.Context, jobChan chan<- job.Job) error {
	p.logger.Infof("StandardPoller started for queues: %v", p.queues)

	for {
		select {
		case <-ctx.Done():
			close(jobChan)
			p.logger.Info("StandardPoller stopped")
			return nil
		default:
			job, err := p.pollOnce(ctx)
			if err != nil {
				p.logger.Errorf("Error polling: %v", err)
				// Brief pause on error
				time.Sleep(time.Second)
				continue
			}

			if job != nil {
				select {
				case <-ctx.Done():
					// Put job back on queue
					if err := p.broker.Nack(ctx, job, true); err != nil {
						p.logger.Errorf("Error requeueing job: %v", err)
					}
					close(jobChan)
					p.logger.Info("StandardPoller stopped")
					return nil
				case jobChan <- job:
					p.logger.Debugf("Job sent to workers: %s", job.GetClass())
				}
			} else {
				// No job found, wait before polling again
				select {
				case <-ctx.Done():
					close(jobChan)
					p.logger.Info("StandardPoller stopped")
					return nil
				case <-time.After(p.interval):
				}
			}
		}
	}
}

// pollOnce attempts to get a job from the queues
func (p *StandardPoller) pollOnce(ctx context.Context) (job.Job, error) {
	queues := p.getQueueOrder()

	for _, queue := range queues {
		p.logger.Debugf("Checking queue: %s", queue)

		job, err := p.broker.Dequeue(ctx, queue)
		if err != nil {
			return nil, err
		}

		if job != nil {
			p.logger.Debugf("Found job on %s: %s", queue, job.GetClass())
			return job, nil
		}
	}

	return nil, nil
}

// getQueueOrder returns the queue processing order
func (p *StandardPoller) getQueueOrder() []string {
	if p.strictOrder {
		return p.queues
	}

	// Shuffle queues for random order
	queues := make([]string, len(p.queues))
	perm := rand.Perm(len(p.queues))
	for i, v := range perm {
		queues[i] = p.queues[v]
	}
	return queues
}

// SetStrictOrder sets whether to use strict queue ordering
func (p *StandardPoller) SetStrictOrder(strict bool) {
	p.strictOrder = strict
}
