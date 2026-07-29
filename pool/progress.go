package pool

import (
	"log/slog"
	"time"
)

// progressEntry 保存单个任务的进度状态和节流信息。
type progressEntry struct {
	current, total int
	lastFlush      time.Time // 上次写入 DB 的时间
}

// SetProgress 更新指定任务 ID 的进度。
// 在该方法中调用是线程安全的。
// 更新后会同时更新内存缓存、触发 OnProgress 回调。
// DB 写入受 ProgressThrottle 控制。
func (p *TaskPool) SetProgress(taskID string, current, total int) {
	p.progressMu.Lock()

	entry, ok := p.progress[taskID]
	if !ok {
		entry = &progressEntry{}
		p.progress[taskID] = entry
	}
	entry.current = current
	entry.total = total

	now := time.Now()
	shouldFlush := true
	throttle := p.cfg.ProgressThrottle
	if throttle > 0 && !entry.lastFlush.IsZero() && now.Sub(entry.lastFlush) < throttle {
		shouldFlush = false
	}
	if shouldFlush {
		entry.lastFlush = now
	}

	fn := p.onProgress
	p.progressMu.Unlock()

	// 触发回调（无论是否节流，回调都实时触发）
	if fn != nil {
		fn(taskID, current, total)
	}

	// DB 写入受节流控制
	if shouldFlush {
		err := p.backend.UpdateProgress(taskID, current, total)
		if err != nil {
			poolLogger().Error("更新任务进度失败",
				slog.String("id", taskID),
				slog.Int("current", current),
				slog.Int("total", total),
				slog.String("error", err.Error()),
			)
			return
		}
	}

	// 更新指标
	if p.metrics != nil {
		p.metrics.incProgressUpdates()
	}

	poolLogger().Debug("任务进度已更新",
		slog.String("id", taskID),
		slog.Int("current", current),
		slog.Int("total", total),
	)
}

// Progress 返回所有任务进度的快照。
// Key 为任务 ID，value 为 [当前进度, 总进度]。
func (p *TaskPool) Progress() map[string][2]int {
	p.progressMu.RLock()
	defer p.progressMu.RUnlock()
	result := make(map[string][2]int, len(p.progress))
	for k, v := range p.progress {
		result[k] = [2]int{v.current, v.total}
	}
	return result
}
