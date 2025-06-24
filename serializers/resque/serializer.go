package resque

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/benmanns/goworker/job"
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
func (s *JSONSerializer) Serialize(j job.Job) ([]byte, error) {
	// For Resque compatibility, we only serialize the payload
	payload := j.GetPayload()

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job: %w", err)
	}

	return data, nil
}

// Deserialize converts JSON bytes to a job
func (s *JSONSerializer) Deserialize(data []byte, metadata job.Metadata) (job.Job, error) {
	var payload job.Payload

	decoder := json.NewDecoder(bytes.NewReader(data))
	if s.useNumber {
		decoder.UseNumber()
	}

	if err := decoder.Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	j := &Job{
		metadata: metadata,
		payload:  payload,
	}

	return j, nil
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
