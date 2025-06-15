package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
)

// JSONSerializer implements the Serializer interface for JSON
type JSONSerializer struct {
	useNumber bool
}

// NewSerializer creates a new JSON serializer
func NewSerializer() *JSONSerializer {
	return &JSONSerializer{
		useNumber: false,
	}
}

// Serialize converts a job to JSON bytes
func (s *JSONSerializer) Serialize(job interfaces.Job) ([]byte, error) {
	// For Resque compatibility, we only serialize the payload
	payload := job.GetPayload()

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job: %w", err)
	}

	return data, nil
}

// Deserialize converts JSON bytes to a job
func (s *JSONSerializer) Deserialize(data []byte, metadata interfaces.JobMetadata) (interfaces.Job, error) {
	var payload interfaces.Payload

	decoder := json.NewDecoder(bytes.NewReader(data))
	if s.useNumber {
		decoder.UseNumber()
	}

	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	job := &Job{
		metadata: metadata,
		payload:  payload,
	}

	return job, nil
}

// GetFormat returns the serialization format name
func (s *JSONSerializer) GetFormat() string {
	return "json"
}

// UseNumber returns whether to use json.Number
func (s *JSONSerializer) UseNumber() bool {
	return s.useNumber
}

// SetUseNumber sets whether to use json.Number
func (s *JSONSerializer) SetUseNumber(useNumber bool) {
	s.useNumber = useNumber
}

// Job represents a deserialized JSON job
type Job struct {
	metadata interfaces.JobMetadata
	payload  interfaces.Payload
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
