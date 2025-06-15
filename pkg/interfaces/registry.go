package interfaces

// WorkerFunc is the function signature for workers
type WorkerFunc func(queue string, args ...interface{}) error

// Registry interface for worker function registration
type Registry interface {
	// Register adds a worker function for a class
	Register(class string, worker WorkerFunc) error

	// Get retrieves a worker function by class
	Get(class string) (WorkerFunc, bool)

	// List returns all registered classes
	List() []string

	// Remove unregisters a worker function
	Remove(class string) error

	// Clear removes all registered workers
	Clear()
}
