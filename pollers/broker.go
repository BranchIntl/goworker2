package pollers

import (
	"context"

	"github.com/BranchIntl/goworker2/job"
)

type Broker interface {
	Dequeue(ctx context.Context, queue string) (job.Job, error)
	Nack(ctx context.Context, job job.Job, requeue bool) error
}
