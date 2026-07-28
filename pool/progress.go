package pool

import "log/slog"

// SetProgress 更新指定任务 ID 的进度。
// 该方法可在 Handler 中安全调用。
// 更新后会自动触发进度回调（OnProgress）。
func (p *TaskPool) SetProgress(taskID string, current, total int) {
	p.progressMu.Lock()
	p.progress[taskID] = [2]int{current, total}
	p.mu.Lock()
	fn := p.onProgress
	p.mu.Unlock()
	p.progressMu.Unlock()

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

	poolLogger().Debug("任务进度已更新",
		slog.String("id", taskID),
		slog.Int("current", current),
		slog.Int("total", total),
	)
	if fn != nil {
		fn(taskID, current, total)
	}
}
