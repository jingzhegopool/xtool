package pool

import (
	"log/slog"
	"os"
	"sync"
)

var (
	logMu  sync.RWMutex
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
)

// SetLogger 设置 pool 包使用的日志记录器。
// 传入 nil 会被忽略，保持当前记录器不变。
func SetLogger(l *slog.Logger) {
	if l == nil {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	logger = l
}

// Logger 返回当前使用的日志记录器。
func Logger() *slog.Logger {
	logMu.RLock()
	defer logMu.RUnlock()
	return logger
}

func poolLogger() *slog.Logger {
	logMu.RLock()
	defer logMu.RUnlock()
	return logger
}
