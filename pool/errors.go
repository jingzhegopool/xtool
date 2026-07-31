package pool

import "errors"

var (
	ErrQueueFull     = errors.New("pool: 队列已满")
	ErrQueueClosed   = errors.New("pool: 队列已关闭")
	ErrUnknownType   = errors.New("pool: 未知的任务类型")
	ErrPoolStopped   = errors.New("pool: 任务池已停止")
	ErrTaskNotFound  = errors.New("pool: 任务未找到")
	ErrUnsupported   = errors.New("pool: 不支持的存储后端")
	ErrInvalidConfig = errors.New("pool: 无效的配置")
	ErrModeConflict  = errors.New("pool: stop_on_error 错误策略需要 sequential 串行模式")
	ErrTaskNotRunning = errors.New("pool: 任务不在运行中")
	ErrTaskNotStartable = errors.New("pool: 任务当前状态不可启动")
	ErrTaskNotRemovable = errors.New("pool: 任务当前状态不可移除")
)
