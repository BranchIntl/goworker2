package redis

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
)

// Job represents a Redis job
type Job struct {
	metadata interfaces.JobMetadata
	payload  interfaces.Payload
}

// NewJob creates a new Redis job
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

// generateJobID generates a unique job ID
func generateJobID() string {
	// Simple implementation - in production might use UUID
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63())
}
