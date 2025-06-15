package interfaces

// Serializer interface for job serialization
type Serializer interface {
	// Serialize converts a job to bytes
	Serialize(job Job) ([]byte, error)

	// Deserialize converts bytes to a job
	Deserialize(data []byte, metadata JobMetadata) (Job, error)

	// GetFormat returns the serialization format name
	GetFormat() string

	// UseNumber determines if numbers should be decoded as json.Number
	UseNumber() bool
	SetUseNumber(useNumber bool)
}
