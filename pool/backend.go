package pool

import (
	"context"
	"time"
)

// Backend 是可插拔的存储后端接口，用于任务的持久化。
// 实现：memory（内存）、sqlite（SQLite）、mysql（MySQL）。
type Backend interface {
	// Init 打开/创建存储后端，并在需要时自动创建表结构。
	Init(ctx context.Context) error

	// Close 清理后端资源。
	Close() error

	// Save 持久化一个新任务。如果 ID 为空，则自动分配一个。
	Save(task *Task) error

	// Get 根据 ID 获取任务。
	Get(id string) (*Task, error)

	// Delete 删除一个任务。
	Delete(id string) error

	// Enqueue 将任务加入待处理队列。
	Enqueue(task *Task) error

	// Dequeue 阻塞等待直到有可用任务，或 ctx 被取消。
	// 仅返回 ScheduledAt <= 当前时间 且 Status 为 pending 的任务。
	Dequeue(ctx context.Context) (*Task, error)

	// DequeueTimeout 类似 Dequeue，但带超时限制。
	DequeueTimeout(ctx context.Context, timeout time.Duration) (*Task, error)

	// Pending 返回待处理任务的数量（不包括延迟任务）。
	Pending() int

	// Remove 按 ID 从队列中移除一个待处理任务。
	Remove(id string) bool

	// UpdateStatus 更新任务的状态和可选的错误信息。
	UpdateStatus(id string, status TaskStatus, errStr string) error

	// UpdateProgress 更新正在执行的任务的进度。
	UpdateProgress(id string, current, total int) error

	// UpdateResult 设置已完成任务的结果。
	UpdateResult(id string, result []byte) error

	// ListByBatchID 返回指定批次 ID 的所有任务。
	ListByBatchID(batchID string) ([]*Task, error)

	// ListByStatus 返回指定状态的任务列表。
	ListByStatus(status TaskStatus, limit, offset int) ([]*Task, error)

	// ListAll 返回所有任务，按创建时间倒序。
	ListAll(limit, offset int) ([]*Task, error)

	// CountByStatus 返回各状态的任务数量统计。
	CountByStatus() (map[TaskStatus]int, error)

	// CancelBatch 取消指定批次中所有待处理的任务。
	CancelBatch(batchID string) (int, error)
}

// backends 注册表
var backends = map[string]func(cfg Config) (Backend, error){}

// registerBackend 注册一个后端构造函数。
func registerBackend(name string, fn func(cfg Config) (Backend, error)) {
	backends[name] = fn
}

// newBackend 根据名称创建后端实例。
func newBackend(cfg Config) (Backend, error) {
	fn, ok := backends[cfg.Backend]
	if !ok {
		return nil, ErrUnsupported
	}
	return fn(cfg)
}
