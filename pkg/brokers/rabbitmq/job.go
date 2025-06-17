package rabbitmq

import (
	"fmt"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/google/uuid"
)

// Job represents an RMQ job
type Job struct {
	metadata interfaces.JobMetadata
	payload  interfaces.Payload
}

// NewJob creates a new RMQ job
func NewJob(queue string, class string, args []interface{}) *Job {
	return &Job{
		metadata: interfaces.JobMetadata{
			ID:         generateJobID(),
			Queue:      queue,
			EnqueuedAt: time.Now(),
		},
		payload: interfaces.Payload{
			Class: class,
			Args:  args,
		},
	}
}

// NewJobWithMetadata creates a new RMQ job with custom metadata
func NewJobWithMetadata(metadata interfaces.JobMetadata, payload interfaces.Payload) *Job {
	return &Job{
		metadata: metadata,
		payload:  payload,
	}
}

// GetID returns the job ID
func (j *Job) GetID() string {
	return j.metadata.ID
}

// GetQueue returns the queue name
func (j *Job) GetQueue() string {
	return j.metadata.Queue
}

// GetClass returns the job class
func (j *Job) GetClass() string {
	return j.payload.Class
}

// GetArgs returns the job arguments
func (j *Job) GetArgs() []interface{} {
	return j.payload.Args
}

// GetEnqueuedAt returns when the job was enqueued
func (j *Job) GetEnqueuedAt() time.Time {
	return j.metadata.EnqueuedAt
}

// GetRetryCount returns the retry count
func (j *Job) GetRetryCount() int {
	return j.metadata.RetryCount
}

// GetLastError returns the last error
func (j *Job) GetLastError() string {
	return j.metadata.LastError
}

// SetRetryCount sets the retry count
func (j *Job) SetRetryCount(count int) {
	j.metadata.RetryCount = count
}

// SetLastError sets the last error
func (j *Job) SetLastError(err string) {
	j.metadata.LastError = err
}

// GetPayload returns the job payload
func (j *Job) GetPayload() interfaces.Payload {
	return j.payload
}

// GetMetadata returns the complete job metadata
func (j *Job) GetMetadata() interfaces.JobMetadata {
	return j.metadata
}

// SetMetadata sets the job metadata
func (j *Job) SetMetadata(metadata interfaces.JobMetadata) {
	j.metadata = metadata
}

// GetPriority returns the job priority
func (j *Job) GetPriority() int {
	return j.metadata.Priority
}

// SetPriority sets the job priority
func (j *Job) SetPriority(priority int) {
	j.metadata.Priority = priority
}

// GetScheduledAt returns when the job is scheduled to run
func (j *Job) GetScheduledAt() *time.Time {
	return j.metadata.ScheduledAt
}

// SetScheduledAt sets when the job should be scheduled to run
func (j *Job) SetScheduledAt(scheduledAt *time.Time) {
	j.metadata.ScheduledAt = scheduledAt
}

// IsScheduled returns true if the job is scheduled for future execution
func (j *Job) IsScheduled() bool {
	return j.metadata.ScheduledAt != nil && j.metadata.ScheduledAt.After(time.Now())
}

// Clone creates a deep copy of the job
func (j *Job) Clone() *Job {
	// Deep copy args slice
	args := make([]interface{}, len(j.payload.Args))
	copy(args, j.payload.Args)

	// Copy scheduled time if present
	var scheduledAt *time.Time
	if j.metadata.ScheduledAt != nil {
		t := *j.metadata.ScheduledAt
		scheduledAt = &t
	}

	return &Job{
		metadata: interfaces.JobMetadata{
			ID:          j.metadata.ID,
			Queue:       j.metadata.Queue,
			EnqueuedAt:  j.metadata.EnqueuedAt,
			RetryCount:  j.metadata.RetryCount,
			LastError:   j.metadata.LastError,
			Priority:    j.metadata.Priority,
			ScheduledAt: scheduledAt,
		},
		payload: interfaces.Payload{
			Class: j.payload.Class,
			Args:  args,
		},
	}
}

// String returns a string representation of the job
func (j *Job) String() string {
	return fmt.Sprintf("Job{ID: %s, Queue: %s, Class: %s, Args: %v, RetryCount: %d}",
		j.metadata.ID, j.metadata.Queue, j.payload.Class, j.payload.Args, j.metadata.RetryCount)
}

// generateJobID generates a unique job ID
func generateJobID() string {
	return "rmq-" + uuid.NewString()
}
