package pool

import "time"

// Config controls the task pool behavior.
// All fields have sensible defaults when created via New().
type Config struct {
	// MinWorkers is the minimum number of idle workers to keep. Default 5.
	MinWorkers int `json:"min_workers"`

	// MaxWorkers is the maximum number of workers for scaling. Default 50.
	MaxWorkers int `json:"max_workers"`

	// QueueCap is the task queue capacity. Default 100000.
	QueueCap int `json:"queue_cap"`

	// DefaultTimeout per task. Default 5 minutes.
	DefaultTimeout time.Duration `json:"default_timeout"`

	// DefaultRetries after a task failure. Default 3.
	DefaultRetries int `json:"default_retries"`

	// DequeueTimeout for workers waiting on empty queue.
	// Idle workers scale down after this timeout. Default 30s.
	DequeueTimeout time.Duration `json:"dequeue_timeout"`

	// ScalerInterval for checking queue depth and scaling. Default 10s.
	ScalerInterval time.Duration `json:"scaler_interval"`
}

// defaultConfig returns safe defaults with zero external dependencies.
func defaultConfig() Config {
	return Config{
		MinWorkers:     5,
		MaxWorkers:     50,
		QueueCap:       100000,
		DefaultTimeout: 5 * time.Minute,
		DefaultRetries: 3,
		DequeueTimeout: 30 * time.Second,
		ScalerInterval: 10 * time.Second,
	}
}
