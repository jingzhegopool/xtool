package pool

// SetProgress updates the progress for a task with the given ID.
// This method is safe to call from within a Handler.
// The progress callback (OnProgress) is fired after updating.
func (p *TaskPool) SetProgress(taskID string, current, total int) {
	p.progressMu.Lock()
	p.progress[taskID] = [2]int{current, total}
	p.mu.Lock()
	fn := p.onProgress
	p.mu.Unlock()
	p.progressMu.Unlock()

	p.backend.UpdateProgress(taskID, current, total)

	if fn != nil {
		fn(taskID, current, total)
	}
}
