package pool

import (
	"context"
	"time"
)

// Backend is the pluggable storage interface for task persistence.
// Implementations: memory, sqlite, mysql.
type Backend interface {
	// Init opens/creates the storage backend and creates tables if needed.
	Init(ctx context.Context) error

	// Close cleans up resources.
	Close() error

	// Save persists a new task. If ID is empty, assign one.
	Save(task *Task) error

	// Get retrieves a task by ID.
	Get(id string) (*Task, error)

	// Delete removes a task.
	Delete(id string) error

	// Enqueue adds a task to the pending queue.
	Enqueue(task *Task) error

	// Dequeue blocks until a pending task is available or ctx is cancelled.
	// Only returns tasks where ScheduledAt <= now and Status == pending.
	Dequeue(ctx context.Context) (*Task, error)

	// DequeueTimeout is like Dequeue but with a timeout.
	DequeueTimeout(ctx context.Context, timeout time.Duration) (*Task, error)

	// Pending returns the count of pending tasks (not delayed).
	Pending() int

	// Remove removes a pending task by ID from the queue.
	Remove(id string) bool

	// UpdateStatus updates a task's status and optional error.
	UpdateStatus(id string, status TaskStatus, errStr string) error

	// UpdateProgress updates progress for a running task.
	UpdateProgress(id string, current, total int) error

	// UpdateResult sets the result for a completed task.
	UpdateResult(id string, result []byte) error

	// ListByBatchID returns all tasks for a given batch ID.
	ListByBatchID(batchID string) ([]*Task, error)

	// ListByStatus returns tasks with the given status.
	ListByStatus(status TaskStatus, limit, offset int) ([]*Task, error)

	// ListAll returns all tasks, newest first.
	ListAll(limit, offset int) ([]*Task, error)

	// CountByStatus returns the count of tasks per status.
	CountByStatus() (map[TaskStatus]int, error)

	// CancelBatch cancels all pending tasks in a batch.
	CancelBatch(batchID string) (int, error)
}

// backends registry
var backends = map[string]func(cfg Config) (Backend, error){}

// registerBackend registers a backend constructor.
func registerBackend(name string, fn func(cfg Config) (Backend, error)) {
	backends[name] = fn
}

// newBackend creates a backend by name.
func newBackend(cfg Config) (Backend, error) {
	fn, ok := backends[cfg.Backend]
	if !ok {
		return nil, ErrUnsupported
	}
	return fn(cfg)
}
