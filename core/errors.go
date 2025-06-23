package core

import "errors"

// Common errors
var (
	// ErrNoQueues is returned when no queues are configured
	ErrNoQueues = errors.New("no queues configured")

	// ErrNoBroker is returned when no broker is configured
	ErrNoBroker = errors.New("no broker configured")

	// ErrNoStatistics is returned when no statistics backend is configured
	ErrNoStatistics = errors.New("no statistics backend configured")

	// ErrNoRegistry is returned when no registry is configured
	ErrNoRegistry = errors.New("no registry configured")

	// ErrNoSerializer is returned when no serializer is configured
	ErrNoSerializer = errors.New("no serializer configured")

	// ErrAlreadyStarted is returned when trying to start an already running engine
	ErrAlreadyStarted = errors.New("engine already started")

	// ErrNotStarted is returned when trying to stop a non-running engine
	ErrNotStarted = errors.New("engine not started")

	// ErrWorkerNotFound is returned when a worker function is not found
	ErrWorkerNotFound = errors.New("worker function not found")

	// ErrInvalidConfiguration is returned when configuration is invalid
	ErrInvalidConfiguration = errors.New("invalid configuration")
)

// JobError wraps job execution errors
type JobError struct {
	Job   string
	Queue string
	Err   error
}

func (e *JobError) Error() string {
	return "job error: " + e.Err.Error()
}

func (e *JobError) Unwrap() error {
	return e.Err
}
