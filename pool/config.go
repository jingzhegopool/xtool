package pool

import "time"

// Config 控制任务池的行为配置。
type Config struct {
	// Backend 指定存储后端类型："memory"、"sqlite" 或 "mysql"。
	// 默认值："memory"。
	Backend string `json:"backend"`

	// DSN 是数据库后端的数据源名称。
	// SQLite："./tasks.db"
	// MySQL：  "user:pass@tcp(127.0.0.1:3306)/dbname"
	DSN string `json:"dsn"`

	// Mode 控制执行模式："parallel"（并发）或 "sequential"（串行）。
	// 默认值："parallel"。
	Mode string `json:"mode"`

	// MaxWorkers 是最大并发工作协程数。
	// 串行模式下固定为 1。
	// 默认值：5。
	MaxWorkers int `json:"max_workers"`

	// OnError 控制任务失败时的行为：
	//   "continue" - 记录错误并继续执行下一个任务
	//   "stop"     - 停止处理并取消当前批次中剩余的待执行任务
	// 默认值："continue"。
	OnError string `json:"on_error"`

	// MaxRetries 是任务的默认重试次数。
	// 默认值：0（不重试）。
	MaxRetries int `json:"max_retries"`

	// DefaultTimeout 是每个任务的默认执行超时时间。
	// 默认值：0（无超时）。
	DefaultTimeout time.Duration `json:"default_timeout"`

	// PollInterval 是数据库后端轮询新任务的间隔时间。
	// 仅用于 SQLite/MySQL 后端（内存后端使用基于通道的阻塞机制）。
	// 默认值：200ms。
	PollInterval time.Duration `json:"poll_interval"`

	// MaxQueueSize 是内存后端的最大队列大小。默认值：100000。
	MaxQueueSize int `json:"max_queue_size"`

	// BatchCompleteCallback 若为 true，则当批次内所有任务完成时触发 OnBatchComplete 回调。
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
	// 串行模式强制 MaxWorkers = 1
	if def.Mode == "sequential" {
		def.MaxWorkers = 1
	}
	return def
}
