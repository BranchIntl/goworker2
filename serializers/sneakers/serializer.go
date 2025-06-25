package sneakers

import (
	"bytes"
	"encoding/json"

	"github.com/BranchIntl/goworker2/errors"
	"github.com/BranchIntl/goworker2/job"
)

// SneakersSerializer implements the Serializer interface for Sneakers format
type SneakersSerializer struct {
	useNumber bool
}

// NewSerializer creates a new Sneakers serializer
func NewSerializer() *SneakersSerializer {
	return &SneakersSerializer{
		useNumber: false,
	}
}

// Serialize converts a job to JSON bytes
func (s *SneakersSerializer) Serialize(j job.Job) ([]byte, error) {
	message := ConstructMessage(j)

	data, err := json.Marshal(message)
	if err != nil {
		return nil, errors.NewSerializationError(s.GetFormat(), err)
	}

	return data, nil
}

// Deserialize converts JSON bytes to a job
func (s *SneakersSerializer) Deserialize(data []byte, _ job.Metadata) (job.Job, error) {
	var message Message

	decoder := json.NewDecoder(bytes.NewReader(data))
	if s.useNumber {
		decoder.UseNumber()
	}

	if err := decoder.Decode(&message); err != nil {
		return nil, errors.NewSerializationError(s.GetFormat(), err)
	}

	j := &Job{
		metadata: ConstructMetadata(message),
		payload:  ConstructPayload(message),
	}

	return j, nil
}

// GetFormat returns the serialization format name
func (s *SneakersSerializer) GetFormat() string {
	return "json"
}

// UseNumber returns whether to use json.Number
func (s *SneakersSerializer) UseNumber() bool {
	return s.useNumber
}

// SetUseNumber sets whether to use json.Number
func (s *SneakersSerializer) SetUseNumber(useNumber bool) {
	s.useNumber = useNumber
}
