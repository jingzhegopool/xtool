// Package pool provides a zero-dependency goroutine task pool with priority queue.
//
// Usage:
//
//	p := pool.New(pool.Config{MinWorkers: 5, MaxWorkers: 50})
//	defer p.Stop()
//
//	p.Handle("email", func(ctx context.Context, job *pool.Job) (any, error) {
//	    return "sent", nil
//	})
//
//	id, _ := p.Submit("email", map[string]string{"to": "user@example.com"},
//	    pool.WithPriority(1))
//
//	stats := p.Stats()
package pool

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"
)

// TaskPool is the user-facing entry point for the task pool.
// Create via New(); all methods are safe for concurrent use.
type TaskPool struct {
	pool    *pool
	started atomic.Bool
}

// New creates a new task pool.
// cfg is optional; an empty Config uses all defaults.
func New(cfg ...Config) *TaskPool {
	c := defaultConfig()
	if len(cfg) > 0 {
		c = applyConfig(c, cfg[0])
	}
	tp := &TaskPool{pool: newPool(c)}
	tp.pool.start()
	tp.started.Store(true)
	return tp
}

// Handle registers a handler for a task type.
// Re-registering the same type overwrites the previous handler.
func (tp *TaskPool) Handle(typ string, handler Handler) {
	tp.pool.registerHandler(typ, handler)
}

// HandleFunc is a convenience wrapper for Handle with a function literal.
func (tp *TaskPool) HandleFunc(typ string, fn func(ctx context.Context, job *Job) (any, error)) {
	tp.pool.registerHandler(typ, fn)
}

// Submit enqueues a new task and returns its ID.
// data is JSON-serialized as the task payload. opts can set priority, timeout, etc.
func (tp *TaskPool) Submit(typ string, data any, opts ...SubmitOption) (string, error) {
	job := &Job{
		Type:       typ,
		Data:       marshalAny(data),
		CreatedAt:  time.Now(),
		MaxRetries: tp.pool.cfg.DefaultRetries,
		Timeout:    tp.pool.cfg.DefaultTimeout,
	}
	for _, opt := range opts {
		opt(job)
	}
	err := tp.pool.submit(job)
	return job.ID, err
}

// SubmitJob enqueues a pre-constructed Job directly.
// Unset fields are filled with pool defaults.
func (tp *TaskPool) SubmitJob(job *Job) error {
	return tp.pool.submit(job)
}

// Stop gracefully shuts down the pool:
// - stops consuming new tasks
// - waits for running tasks to complete or timeout
// - all worker goroutines exit
func (tp *TaskPool) Stop() {
	tp.pool.stop()
}

// Pause suspends dequeuing new tasks. Running tasks finish normally.
// Call Resume() to continue.
func (tp *TaskPool) Pause() {
	tp.pool.pause()
}

// Resume resumes dequeuing tasks from the queue.
func (tp *TaskPool) Resume() {
	tp.pool.resume()
}

// Cancel removes a pending job from the queue by ID.
// Returns true if the job was found and removed.
func (tp *TaskPool) Cancel(id string) bool {
	return tp.pool.cancelJob(id)
}

// Stats returns a snapshot of the current pool state.
func (tp *TaskPool) Stats() PoolStats {
	return tp.pool.statsSnapshot()
}

// OnComplete registers a callback for successfully completed tasks.
func (tp *TaskPool) OnComplete(fn func(*Job)) {
	tp.pool.onComplete = fn
}

// OnFailed registers a callback for tasks that ultimately failed (no more retries).
func (tp *TaskPool) OnFailed(fn func(*Job, error)) {
	tp.pool.onFailed = fn
}

// --- SubmitOption ---

// SubmitOption configures a task submission.
type SubmitOption func(*Job)

// WithPriority sets the task priority. Lower numbers execute first.
func WithPriority(p int) SubmitOption {
	return func(job *Job) { job.Priority = p }
}

// WithTimeout sets the execution timeout for this task.
func WithTimeout(d time.Duration) SubmitOption {
	return func(job *Job) { job.Timeout = d }
}

// WithRetries sets the maximum retry count on failure.
func WithRetries(n int) SubmitOption {
	return func(job *Job) { job.MaxRetries = n }
}

// WithJobID sets a custom job ID. Auto-generated UUID v4 if not set.
func WithJobID(id string) SubmitOption {
	return func(job *Job) { job.ID = id }
}

// --- internal helpers ---

func marshalAny(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{"__marshal_error__":"` + err.Error() + `"}`)
	}
	return json.RawMessage(b)
}

func applyConfig(def, user Config) Config {
	if user.MinWorkers > 0 {
		def.MinWorkers = user.MinWorkers
	}
	if user.MaxWorkers > 0 {
		def.MaxWorkers = user.MaxWorkers
	}
	if user.QueueCap > 0 {
		def.QueueCap = user.QueueCap
	}
	if user.DefaultTimeout > 0 {
		def.DefaultTimeout = user.DefaultTimeout
	}
	if user.DefaultRetries > 0 {
		def.DefaultRetries = user.DefaultRetries
	}
	if user.DequeueTimeout > 0 {
		def.DequeueTimeout = user.DequeueTimeout
	}
	if user.ScalerInterval > 0 {
		def.ScalerInterval = user.ScalerInterval
	}
	if def.MinWorkers > def.MaxWorkers {
		def.MinWorkers = def.MaxWorkers
	}
	return def
}
