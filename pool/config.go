package pool

import "time"

// Config controls the task pool behavior.
type Config struct {
	// Backend specifies the storage backend: "memory", "sqlite", or "mysql".
	// Default: "memory".
	Backend string `json:"backend"`

	// DSN is the data source name for database backends.
	// SQLite: "./tasks.db"
	// MySQL:  "user:pass@tcp(127.0.0.1:3306)/dbname"
	DSN string `json:"dsn"`

	// Mode controls execution: "parallel" or "sequential".
	// Default: "parallel".
	Mode string `json:"mode"`

	// MaxWorkers is the maximum number of concurrent workers.
	// In sequential mode this is always 1.
	// Default: 5.
	MaxWorkers int `json:"max_workers"`

	// OnError controls behavior on task failure:
	//   "continue" - log error and continue to next task
	//   "stop"     - stop processing and cancel remaining pending tasks in current batch
	// Default: "continue".
	OnError string `json:"on_error"`

	// MaxRetries is the default retry count for tasks.
	// Default: 0 (no retry).
	MaxRetries int `json:"max_retries"`

	// DefaultTimeout is the default execution timeout per task.
	// Default: 0 (no timeout).
	DefaultTimeout time.Duration `json:"default_timeout"`

	// PollInterval is how often database backends poll for new tasks.
	// Only used for SQLite/MySQL backends (memory uses channel-based blocking).
	// Default: 200ms.
	PollInterval time.Duration `json:"poll_interval"`

	// MaxQueueSize for memory backend. Default: 100000.
	MaxQueueSize int `json:"max_queue_size"`

	// BatchCompleteCallback if true, fires OnBatchComplete when all tasks in a batch are done.
	BatchCompleteCallback bool `json:"batch_complete_callback"`
}

func defaultConfig() Config {
	return Config{
		Backend:              "memory",
		Mode:                 "parallel",
		MaxWorkers:           5,
		OnError:              "continue",
		MaxRetries:           0,
		PollInterval:         200 * time.Millisecond,
		MaxQueueSize:         100000,
		BatchCompleteCallback: true,
	}
}

func applyConfig(def Config, user Config) Config {
	if user.Backend != "" {
		def.Backend = user.Backend
	}
	if user.DSN != "" {
		def.DSN = user.DSN
	}
	if user.Mode != "" {
		def.Mode = user.Mode
	}
	if user.MaxWorkers > 0 {
		def.MaxWorkers = user.MaxWorkers
	}
	if user.OnError != "" {
		def.OnError = user.OnError
	}
	if user.MaxRetries > 0 {
		def.MaxRetries = user.MaxRetries
	}
	if user.DefaultTimeout > 0 {
		def.DefaultTimeout = user.DefaultTimeout
	}
	if user.PollInterval > 0 {
		def.PollInterval = user.PollInterval
	}
	if user.MaxQueueSize > 0 {
		def.MaxQueueSize = user.MaxQueueSize
	}
	// Sequential mode forces MaxWorkers = 1
	if def.Mode == "sequential" {
		def.MaxWorkers = 1
	}
	return def
}
