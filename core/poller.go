package core

import (
	"context"
	"math/rand"
	"time"

	"github.com/BranchIntl/goworker2/job"
	"github.com/cihub/seelog"
)

// Poller is a broker-agnostic job poller
type Poller struct {
	broker      Broker
	stats       Statistics
	queues      []string
	interval    time.Duration
	jobChan     chan<- job.Job
	logger      seelog.LoggerInterface
	strictOrder bool
}

// NewPoller creates a new poller
func NewPoller(
	broker Broker,
	stats Statistics,
	queues []string,
	interval time.Duration,
	jobChan chan<- job.Job,
	logger seelog.LoggerInterface,
) *Poller {
	return &Poller{
		broker:      broker,
		stats:       stats,
		queues:      queues,
		interval:    interval,
		jobChan:     jobChan,
		logger:      logger,
		strictOrder: true,
	}
}

// Start begins polling for jobs
func (p *Poller) Start(ctx context.Context) error {
	p.logger.Infof("Poller started for queues: %v", p.queues)

	for {
		select {
		case <-ctx.Done():
			close(p.jobChan)
			p.logger.Info("Poller stopped")
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
					close(p.jobChan)
					p.logger.Info("Poller stopped")
					return nil
				case p.jobChan <- job:
					p.logger.Debugf("Job sent to workers: %s", job.GetClass())
				}
			} else {
				// No job found, wait before polling again
				select {
				case <-ctx.Done():
					close(p.jobChan)
					p.logger.Info("Poller stopped")
					return nil
				case <-time.After(p.interval):
				}
			}
		}
	}
}

// pollOnce attempts to get a job from the queues
func (p *Poller) pollOnce(ctx context.Context) (job.Job, error) {
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
func (p *Poller) getQueueOrder() []string {
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
func (p *Poller) SetStrictOrder(strict bool) {
	p.strictOrder = strict
}
