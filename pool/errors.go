package pool

import "errors"

var (
	// ErrQueueFull indicates the queue has reached its capacity.
	ErrQueueFull = errors.New("pool: queue is full")

	// ErrQueueClosed indicates the queue has been closed.
	ErrQueueClosed = errors.New("pool: queue is closed")

	// ErrUnknownType indicates no handler has been registered for the task type.
	ErrUnknownType = errors.New("pool: unknown task type")

	// ErrPoolStopped indicates the pool has been stopped.
	ErrPoolStopped = errors.New("pool: pool is stopped")

	// ErrJobNotFound indicates the job ID was not found.
	ErrJobNotFound = errors.New("pool: job not found")
)
