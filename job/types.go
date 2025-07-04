package job

import "time"

// Job interface represents a job to be processed
type Job interface {
	// Basic job information
	GetID() string
	GetQueue() string
	GetClass() string
	GetArgs() []interface{}

	// Job metadata
	GetMetadata() Metadata
	GetEnqueuedAt() time.Time
	GetRetryCount() int
	GetLastError() string

	// Job control
	SetRetryCount(count int)
	SetLastError(err string)

	// Serialization helper
	GetPayload() Payload
}

// Payload represents the job payload
type Payload struct {
	Class string        `json:"class"`
	Args  []interface{} `json:"args"`
}

// Metadata contains additional job information
type Metadata struct {
	ID          string
	Queue       string
	EnqueuedAt  time.Time
	RetryCount  int
	LastError   string
	Priority    int
	ScheduledAt *time.Time
	Locale      string
	Timezone    string
}
