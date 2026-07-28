package pool

import (
	"encoding/json"
	"time"
)

// JobStatus represents the lifecycle status of a task.
type JobStatus int

const (
	StatusPending   JobStatus = iota // waiting in queue
	StatusRunning                    // being executed by a worker
	StatusCompleted                  // executed successfully
	StatusFailed                     // execution failed
	StatusCancelled                  // cancelled before execution
)

func (s JobStatus) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// Job represents a unit of work to be processed.
type Job struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data,omitempty"`
	Status  JobStatus       `json:"status"`
	Version int             `json:"version"` // optimistic lock version (reserved)

	Priority   int           `json:"priority"`
	Timeout    time.Duration `json:"timeout,omitempty"`
	MaxRetries int           `json:"max_retries"`
	Retries    int           `json:"retries"`

	CreatedAt time.Time `json:"created_at"`
	StartedAt time.Time `json:"started_at,omitempty"`
	DoneAt    time.Time `json:"done_at,omitempty"`

	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// Decode unmarshals the task data into the given value.
func (j *Job) Decode(v any) error {
	return json.Unmarshal(j.Data, v)
}
