package memory

// Options for memory broker
type Options struct {
	// QueueSize is the buffer size for each queue channel
	QueueSize int
}

// DefaultOptions returns default memory broker options
func DefaultOptions() Options {
	return Options{
		QueueSize: 1000,
	}
}
