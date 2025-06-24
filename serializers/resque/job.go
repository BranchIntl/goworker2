package resque

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/benmanns/goworker/job"
)

// Job represents a deserialized resque job
type Job struct {
	metadata job.Metadata
	payload  job.Payload
}

// NewJob creates a new resque job
func NewJob(queue string, class string, args []interface{}) *Job {
	return &Job{
		metadata: job.Metadata{
			ID:         generateJobID(),
			Queue:      queue,
			EnqueuedAt: time.Now(),
		},
		payload: job.Payload{
			Class: class,
			Args:  args,
		},
	}
}

// NewJobWithMetadata creates a new resque job with custom metadata
func NewJobWithMetadata(metadata job.Metadata, payload job.Payload) *Job {
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
func (j *Job) GetPayload() job.Payload {
	return j.payload
}

func (j *Job) GetMetadata() job.Metadata {
	return j.metadata
}

// generateJobID generates a unique job ID
func generateJobID() string {
	// Simple implementation - in production might use UUID
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63())
}
