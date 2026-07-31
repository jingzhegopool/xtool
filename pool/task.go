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
	StatusPaused                      // 7 - 手动暂停，可恢复
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
	case StatusPaused:
		return "paused"
	default:
		return "unknown"
	}
}

// Task 表示一个工作单元。
type Task struct {
	// ID 是任务的唯一标识，为空时由后端自动生成 UUID。
	ID string `json:"id"`

	// Type 是任务类型，用于匹配对应的 Handler。
	Type string `json:"type"`

	// Data 是任务的负载数据，由用户自行定义格式。
	Data json.RawMessage `json:"data,omitempty"`

	// Status 是任务当前的生命周期状态。
	Status TaskStatus `json:"status"`

	// Priority 是任务优先级，数值越小优先级越高。
	Priority int `json:"priority"`

	// BatchID 是任务所属批次 ID，用于批次完成回调。
	BatchID string `json:"batch_id,omitempty"`

	// Timeout 是单个任务的执行超时时间，0 表示不超时。
	Timeout time.Duration `json:"timeout,omitempty"`

	// MaxRetries 是任务失败时的最大重试次数。
	MaxRetries int `json:"max_retries"`

	// Retries 是任务已经过的重试次数。
	Retries int `json:"retries"`

	// ScheduledAt 是任务的计划执行时间，零值表示立即执行。
	ScheduledAt time.Time `json:"scheduled_at,omitempty"`

	// CreatedAt 是任务的创建时间。
	CreatedAt time.Time `json:"created_at"`

	// StartedAt 是任务开始执行的时间。
	StartedAt *time.Time `json:"started_at,omitempty"`

	// DoneAt 是任务到达终态（完成/失败/取消）的时间。
	DoneAt *time.Time `json:"done_at,omitempty"`

	// Error 是任务失败时的错误信息。
	Error string `json:"error,omitempty"`

	// Result 是任务成功执行后的结果数据。
	Result json.RawMessage `json:"result,omitempty"`

	// ProgressCurrent 是任务当前进度值。
	ProgressCurrent int `json:"progress_current"`

	// ProgressTotal 是任务总进度值，0 表示无进度概念。
	ProgressTotal int `json:"progress_total"`

	// Metadata 是用户自定义的 JSON 数据，可用于存储业务字段（如 mall_id、shop_key、
	// 断点恢复进度等）。由用户自行管理内容，xtool/pool 仅负责持久化和读取。
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

// Decode 将任务数据反序列化到给定的值中。
func (t *Task) Decode(v any) error {
	return json.Unmarshal(t.Data, v)
}

// DecodeMetadata 将 Metadata 反序列化到给定的值中。
func (t *Task) DecodeMetadata(v any) error {
	if len(t.Metadata) == 0 {
		return nil
	}
	return json.Unmarshal(t.Metadata, v)
}

// TaskResult 在批次完成回调中返回已完成任务的结果。
type TaskResult struct {
	ID     string          `json:"id"`
	Status TaskStatus      `json:"status"`
	Data   json.RawMessage `json:"data,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}
