package pool

import (
	"encoding/json"
	"time"
)

// TaskStatus 表示任务的生命周期状态。
type TaskStatus int

const (
	StatusPending   TaskStatus = iota // 0 - 等待执行
	StatusDelayed                     // 1 - 等待调度时间到达
	StatusRunning                     // 2 - 正在执行
	StatusCompleted                   // 3 - 执行成功
	StatusFailed                      // 4 - 执行失败（无更多重试机会）
	StatusCancelled                   // 5 - 已取消
	StatusRetrying                    // 6 - 失败，但将重试
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

// Task 表示一个工作单元。
type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data,omitempty"`
	Status    TaskStatus      `json:"status"`
	Priority  int             `json:"priority"`   // 优先级，数值越小优先级越高
	BatchID   string          `json:"batch_id,omitempty"`

	Timeout    time.Duration `json:"timeout,omitempty"`
	MaxRetries int           `json:"max_retries"`
	Retries    int           `json:"retries"`

	ScheduledAt time.Time  `json:"scheduled_at,omitempty"` // 调度执行时间
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	DoneAt      *time.Time `json:"done_at,omitempty"`

	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`

	// 进度跟踪
	ProgressCurrent int `json:"progress_current"`
	ProgressTotal   int `json:"progress_total"`
}

// Decode 将任务数据反序列化到给定的值中。
func (t *Task) Decode(v any) error {
	return json.Unmarshal(t.Data, v)
}

// TaskResult 在批次完成回调中返回已完成任务的结果。
type TaskResult struct {
	ID     string          `json:"id"`
	Status TaskStatus      `json:"status"`
	Data   json.RawMessage `json:"data,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}
