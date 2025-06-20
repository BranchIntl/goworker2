package sneakers

import (
	"encoding/json"
	"time"

	"github.com/benmanns/goworker/pkg/interfaces"
	"github.com/google/uuid"
)

type Job struct {
	metadata interfaces.JobMetadata
	payload  interfaces.Payload
}

type Message struct {
	JobClass            string                 `json:"job_class"`
	JobID               string                 `json:"job_id"`
	ProviderJobID       *string                `json:"provider_job_id"`
	QueueName           string                 `json:"queue_name"`
	Priority            int                    `json:"priority"`
	Arguments           []interface{}          `json:"arguments"`
	Executions          int                    `json:"executions"`
	ExceptionExecutions map[string]interface{} `json:"exception_executions"`
	Locale              string                 `json:"locale"`
	Timezone            string                 `json:"timezone"`
	EnqueuedAt          string                 `json:"enqueued_at"`
}

func NewJob(queue string, class string, args []interface{}) *Job {
	return &Job{
		metadata: interfaces.JobMetadata{
			ID:         uuid.NewString(),
			Queue:      queue,
			EnqueuedAt: time.Now(),
		},
		payload: interfaces.Payload{
			Class: class,
			Args:  args,
		},
	}
}

func (j *Job) GetID() string {
	return j.metadata.ID
}

func (j *Job) GetQueue() string {
	return j.metadata.Queue
}

func (j *Job) GetClass() string {
	return j.payload.Class
}

func (j *Job) GetArgs() []interface{} {
	return j.payload.Args
}

func (j *Job) GetEnqueuedAt() time.Time {
	return j.metadata.EnqueuedAt
}

func (j *Job) GetRetryCount() int {
	return j.metadata.RetryCount
}

func (j *Job) GetLastError() string {
	return j.metadata.LastError
}

func (j *Job) SetRetryCount(count int) {
	j.metadata.RetryCount = count
}

func (j *Job) SetLastError(err string) {
	j.metadata.LastError = err
}

func (j *Job) GetPayload() interfaces.Payload {
	return j.payload
}

func (j *Job) GetMetadata() interfaces.JobMetadata {
	return j.metadata
}

func ConstructMessage(j interfaces.Job) Message {
	payload := j.GetPayload()
	metadata := j.GetMetadata()
	return Message{
		JobClass:            payload.Class,
		JobID:               uuid.NewString(),
		ProviderJobID:       nil,
		QueueName:           metadata.Queue,
		Priority:            metadata.Priority,
		Arguments:           payload.Args,
		Executions:          0,
		ExceptionExecutions: map[string]interface{}{},
		Locale:              metadata.Locale,
		Timezone:            metadata.Timezone,
		EnqueuedAt:          time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	}
}

func ConstructPayload(msg Message) interfaces.Payload {
	return interfaces.Payload{
		Class: msg.JobClass,
		Args:  msg.Arguments,
	}
}

func ConstructMetadata(msg Message) interfaces.JobMetadata {
	var enqueuedAt time.Time
	if msg.EnqueuedAt != "" {
		parsedTime, err := time.Parse("2006-01-02T15:04:05Z", msg.EnqueuedAt)
		if err == nil {
			enqueuedAt = parsedTime
		}
	}
	var lastError string
	if len(msg.ExceptionExecutions) > 0 {
		jsonBytes, err := json.Marshal(msg.ExceptionExecutions)
		if err == nil {
			lastError = string(jsonBytes)
		}
	}
	return interfaces.JobMetadata{
		ID:         msg.JobID,
		Queue:      msg.QueueName,
		Priority:   msg.Priority,
		Locale:     msg.Locale,
		Timezone:   msg.Timezone,
		EnqueuedAt: enqueuedAt,
		RetryCount: msg.Executions,
		LastError:  lastError,
	}
}
