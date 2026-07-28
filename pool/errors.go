package pool

import "errors"

var (
	ErrQueueFull     = errors.New("pool: queue is full")
	ErrQueueClosed   = errors.New("pool: queue is closed")
	ErrUnknownType   = errors.New("pool: unknown task type")
	ErrPoolStopped   = errors.New("pool: pool is stopped")
	ErrTaskNotFound  = errors.New("pool: task not found")
	ErrUnsupported   = errors.New("pool: unsupported backend")
	ErrInvalidConfig = errors.New("pool: invalid config")
	ErrModeConflict  = errors.New("pool: stop_on_error requires sequential mode")
)
