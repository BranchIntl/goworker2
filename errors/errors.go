// Package errors provides error types and utilities for the goworker library.
package errors

import (
	"errors"
	"fmt"
)

// Sentinel errors for common conditions
var (
	ErrNotConnected   = errors.New("not connected")
	ErrQueueNotFound  = errors.New("queue not found")
	ErrQueueFull      = errors.New("queue is full")
	ErrWorkerNotFound = errors.New("worker not found")
	ErrInvalidPayload = errors.New("invalid payload")
	ErrTimeout        = errors.New("operation timed out")
	ErrShutdown       = errors.New("shutting down")
	ErrInvalidConfig  = errors.New("invalid configuration")
	ErrEmptyClassName = errors.New("class name cannot be empty")
	ErrNilWorkerFunc  = errors.New("worker function cannot be nil")
)

// BrokerError represents broker-specific errors
type BrokerError struct {
	Op    string // operation being performed
	Queue string // queue name (if applicable)
	Err   error  // underlying error
}

func (e *BrokerError) Error() string {
	if e.Queue != "" {
		return fmt.Sprintf("broker %s on queue %s: %v", e.Op, e.Queue, e.Err)
	}
	return fmt.Sprintf("broker %s: %v", e.Op, e.Err)
}

func (e *BrokerError) Unwrap() error {
	return e.Err
}

// WorkerError represents worker execution errors
type WorkerError struct {
	Class string // job class
	Queue string // queue name
	Err   error  // underlying error
}

func (e *WorkerError) Error() string {
	return fmt.Sprintf("worker %s on queue %s: %v", e.Class, e.Queue, e.Err)
}

func (e *WorkerError) Unwrap() error {
	return e.Err
}

// SerializationError represents serialization/deserialization errors
type SerializationError struct {
	Format string // serialization format
	Err    error  // underlying error
}

func (e *SerializationError) Error() string {
	return fmt.Sprintf("serialization (%s): %v", e.Format, e.Err)
}

func (e *SerializationError) Unwrap() error {
	return e.Err
}

// ConnectionError represents connection-related errors
type ConnectionError struct {
	URI string // connection URI (may be redacted)
	Err error  // underlying error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection to %s: %v", e.URI, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

func (e *ConnectionError) Temporary() bool {
	// Implement net.Error interface for timeout detection
	if t, ok := e.Err.(interface{ Temporary() bool }); ok {
		return t.Temporary()
	}
	return false
}

func (e *ConnectionError) Timeout() bool {
	// Implement net.Error interface for timeout detection
	if t, ok := e.Err.(interface{ Timeout() bool }); ok {
		return t.Timeout()
	}
	return false
}

// Helper functions for creating errors

// NewBrokerError creates a new broker error
func NewBrokerError(op, queue string, err error) error {
	return &BrokerError{Op: op, Queue: queue, Err: err}
}

// NewWorkerError creates a new worker error
func NewWorkerError(class, queue string, err error) error {
	return &WorkerError{Class: class, Queue: queue, Err: err}
}

// NewSerializationError creates a new serialization error
func NewSerializationError(format string, err error) error {
	return &SerializationError{Format: format, Err: err}
}

// NewConnectionError creates a new connection error
func NewConnectionError(uri string, err error) error {
	return &ConnectionError{URI: uri, Err: err}
}

// IsTemporary checks if an error is temporary and retryable
func IsTemporary(err error) bool {
	if t, ok := err.(interface{ Temporary() bool }); ok {
		return t.Temporary()
	}

	// Check common temporary error conditions
	return errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrQueueFull)
}

// IsTimeout checks if an error is a timeout
func IsTimeout(err error) bool {
	if t, ok := err.(interface{ Timeout() bool }); ok {
		return t.Timeout()
	}
	return errors.Is(err, ErrTimeout)
}
