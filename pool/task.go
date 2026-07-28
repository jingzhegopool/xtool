package pool

import (
	"encoding/json"
	"time"
)

// TaskStatus represents the lifecycle status of a task.
type TaskStatus int

const (
	StatusPending   TaskStatus = iota // waiting in queue
	StatusDelayed                     // waiting for scheduled time
	StatusRunning                     // being executed
	StatusCompleted                   // executed successfully
	StatusFailed                      // execution failed (no more retries)
	StatusCancelled                   // cancelled
	StatusRetrying                    // failed but will retry
)

func (s TaskStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusDelayed:
		return "delayed"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	case StatusRetrying:
		return "retrying"
	default:
		return "unknown"
	}
}

// Task represents a unit of work.
type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data,omitempty"`
	Status    TaskStatus      `json:"status"`
	Priority  int             `json:"priority"`
	BatchID   string          `json:"batch_id,omitempty"`

	Timeout    time.Duration `json:"timeout,omitempty"`
	MaxRetries int           `json:"max_retries"`
	Retries    int           `json:"retries"`

	ScheduledAt time.Time  `json:"scheduled_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	DoneAt      *time.Time `json:"done_at,omitempty"`

	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`

	// Progress tracking
	ProgressCurrent int `json:"progress_current"`
	ProgressTotal   int `json:"progress_total"`
}

// Decode unmarshals the task data into the given value.
func (t *Task) Decode(v any) error {
	return json.Unmarshal(t.Data, v)
}

// TaskResult is returned for completed tasks in batch callbacks.
type TaskResult struct {
	ID     string          `json:"id"`
	Status TaskStatus      `json:"status"`
	Data   json.RawMessage `json:"data,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}
