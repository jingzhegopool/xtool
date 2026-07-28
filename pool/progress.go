package pool

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
		return
	}

	if fn != nil {
		fn(taskID, current, total)
	}
}
