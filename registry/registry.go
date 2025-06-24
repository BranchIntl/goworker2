package registry

import (
	"sync"

	"github.com/benmanns/goworker/core"
	"github.com/benmanns/goworker/errors"
)

// Registry is a thread-safe worker function registry
type Registry struct {
	mu      sync.RWMutex
	workers map[string]core.WorkerFunc
}

// NewRegistry creates a new registry
func NewRegistry() *Registry {
	return &Registry{
		workers: make(map[string]core.WorkerFunc),
	}
}

// Register adds a worker function for a class
func (r *Registry) Register(class string, worker core.WorkerFunc) error {
	if class == "" {
		return errors.ErrEmptyClassName
	}

	if worker == nil {
		return errors.ErrNilWorkerFunc
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.workers[class] = worker
	return nil
}

// Get retrieves a worker function by class
func (r *Registry) Get(class string) (core.WorkerFunc, bool) {
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

	r.workers = make(map[string]core.WorkerFunc)
}
