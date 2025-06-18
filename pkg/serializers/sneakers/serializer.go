package sneakers

import (
	"bytes"
	"encoding/json"
	"fmt"

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
	message := ConstructMessage(job)

	data, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job: %w", err)
	}

	return data, nil
}

// Deserialize converts JSON bytes to a job
func (s *JSONSerializer) Deserialize(data []byte) (interfaces.Job, error) {
	var message Message

	decoder := json.NewDecoder(bytes.NewReader(data))
	if s.useNumber {
		decoder.UseNumber()
	}

	if err := decoder.Decode(&message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	job := &Job{
		metadata: ConstructMetadata(message),
		payload:  ConstructPayload(message),
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