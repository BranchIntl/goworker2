package registry

import (
	"fmt"
	"sync"

	"github.com/benmanns/goworker/pkg/interfaces"
)

// Registry is a thread-safe worker function registry
type Registry struct {
	mu      sync.RWMutex
	workers map[string]interfaces.WorkerFunc
}

// NewRegistry creates a new registry
func NewRegistry() *Registry {
	return &Registry{
		workers: make(map[string]interfaces.WorkerFunc),
	}
}

// Register adds a worker function for a class
func (r *Registry) Register(class string, worker interfaces.WorkerFunc) error {
	if class == "" {
		return fmt.Errorf("class name cannot be empty")
	}

	if worker == nil {
		return fmt.Errorf("worker function cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.workers[class] = worker
	return nil
}

// Get retrieves a worker function by class
func (r *Registry) Get(class string) (interfaces.WorkerFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	worker, ok := r.workers[class]
	return worker, ok
}

// List returns all registered classes
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	classes := make([]string, 0, len(r.workers))
	for class := range r.workers {
		classes = append(classes, class)
	}

	return classes
}

// Remove unregisters a worker function
func (r *Registry) Remove(class string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.workers, class)
	return nil
}

// Clear removes all registered workers
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.workers = make(map[string]interfaces.WorkerFunc)
}
