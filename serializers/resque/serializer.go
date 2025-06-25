package resque

import (
	"bytes"
	"encoding/json"

	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
)

// ResqueSerializer implements the Serializer interface for Resque format
type ResqueSerializer struct {
	useNumber bool
}

// NewSerializer creates a new Resque serializer
func NewSerializer() *ResqueSerializer {
	return &ResqueSerializer{
		useNumber: false,
	}
}

// Serialize converts a job to JSON bytes
func (s *ResqueSerializer) Serialize(j job.Job) ([]byte, error) {
	// For Resque compatibility, we only serialize the payload
	payload := j.GetPayload()

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.NewSerializationError(s.GetFormat(), err)
	}

	return data, nil
}

// Deserialize converts JSON bytes to a job
func (s *ResqueSerializer) Deserialize(data []byte, metadata job.Metadata) (job.Job, error) {
	var payload job.Payload

	decoder := json.NewDecoder(bytes.NewReader(data))
	if s.useNumber {
		decoder.UseNumber()
	}

	if err := decoder.Decode(&payload); err != nil {
		return nil, errors.NewSerializationError(s.GetFormat(), err)
	}

	j := &Job{
		metadata: metadata,
		payload:  payload,
	}

	return j, nil
}

// GetFormat returns the serialization format name
func (s *ResqueSerializer) GetFormat() string {
	return "json"
}

// UseNumber returns whether to use json.Number
func (s *ResqueSerializer) UseNumber() bool {
	return s.useNumber
}

// SetUseNumber sets whether to use json.Number
func (s *ResqueSerializer) SetUseNumber(useNumber bool) {
	s.useNumber = useNumber
}
