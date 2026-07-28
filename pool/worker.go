package pool

import (
	"context"
	"fmt"
	"log"
	"time"
)

// worker is a single goroutine that processes jobs from the queue.
type worker struct {
	id   int
	pool *pool
}

func (w *worker) run() {
	defer func() {
		w.pool.wg.Done()
		w.pool.stats.decIdle()
	}()

	for {
		if w.shouldExit() {
			return
		}
		if checkDone(w.pool.ctx) {
			return
		}

		// Block when paused
		if w.pool.paused.Load() {
			w.waitWhilePaused()
			continue
		}

		dequeueCtx, cancel := context.WithTimeout(w.pool.ctx, w.pool.cfg.DequeueTimeout)
		job, err := w.pool.queue.Dequeue(dequeueCtx)
		cancel()

		if err != nil {
			switch err {
			case context.Canceled, ErrQueueClosed:
				return
			case context.DeadlineExceeded:
				if w.tryScaleDown() {
					return
				}
				continue
			default:
				log.Printf("pool: worker %d dequeue error: %v", w.id, err)
				continue
			}
		}

		w.process(job)
	}
}

// process executes a job with timeout, panic recovery, retry, and callbacks.
func (w *worker) process(job *Job) {
	w.pool.stats.decIdle()
	w.pool.stats.incActive()
	w.pool.stats.incRunning()

	job.Status = StatusRunning
	job.StartedAt = time.Now()

	handler, ok := w.pool.getHandler(job.Type)
	if !ok {
		job.Status = StatusFailed
		job.Error = fmt.Sprintf("no handler registered for type %q", job.Type)
		job.DoneAt = time.Now()
		w.finishJob(job, nil)
		return
	}

	execCtx := w.pool.ctx
	var cancel context.CancelFunc
	if job.Timeout > 0 {
		execCtx, cancel = context.WithTimeout(execCtx, job.Timeout)
		defer cancel()
	}

	var (
		result  any
		execErr error
	)

	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("worker panic: %v", r)
				log.Printf("pool: worker %d recovered panic: %v", w.id, r)
			}
		}()
		result, execErr = handler(execCtx, job)
	}()

	job.DoneAt = time.Now()

	if execErr != nil {
		job.Status = StatusFailed
		job.Error = execErr.Error()
	} else {
		job.Status = StatusCompleted
		job.Result = marshalResult(result)
	}

	w.finishJob(job, execErr)
}

// finishJob handles post-execution: retry, callbacks, and statistics.
func (w *worker) finishJob(job *Job, execErr error) {
	w.pool.stats.decRunning()
	w.pool.stats.incIdle()
	w.pool.stats.decActive()

	if job.Status == StatusFailed && job.Retries < job.MaxRetries {
		job.Retries++
		job.Status = StatusPending
		job.Error = ""
		job.StartedAt = time.Time{}
		job.DoneAt = time.Time{}

		if err := w.pool.queue.Enqueue(job); err != nil {
			job.Status = StatusFailed
			job.Error = fmt.Sprintf("retry enqueue failed: %v (last error: %v)", err, execErr)
			w.pool.stats.incFailed()
			w.pool.fireOnFailed(job, fmt.Errorf("retry: %w", err))
		}
		return
	}

	switch job.Status {
	case StatusCompleted:
		w.pool.stats.incDone()
		w.pool.fireOnComplete(job)
	case StatusFailed:
		w.pool.stats.incFailed()
		if execErr != nil {
			w.pool.fireOnFailed(job, execErr)
		} else {
			w.pool.fireOnFailed(job, fmt.Errorf(job.Error))
		}
	}
}

// shouldExit checks if the worker should exit (scale down).
// Idle workers above MinWorkers exit when there are no pending tasks.
func (w *worker) shouldExit() bool {
	w.pool.mu.Lock()
	defer w.pool.mu.Unlock()

	if w.pool.paused.Load() {
		return false
	}
	if len(w.pool.workers) > w.pool.cfg.MinWorkers && w.pool.queue.Pending() == 0 {
		return w.removeSelf()
	}
	return false
}

// waitWhilePaused blocks while the pool is paused, polling for resume.
func (w *worker) waitWhilePaused() {
	for w.pool.paused.Load() {
		if checkDone(w.pool.ctx) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// tryScaleDown checks if the worker should exit after a dequeue timeout.
func (w *worker) tryScaleDown() bool {
	w.pool.mu.Lock()
	defer w.pool.mu.Unlock()

	if w.pool.paused.Load() || w.pool.queue.Pending() > 0 {
		return false
	}
	if len(w.pool.workers) <= w.pool.cfg.MinWorkers {
		return false
	}
	return w.removeSelf()
}

// removeSelf removes this worker from the pool's worker list.
// Caller must hold p.mu.
func (w *worker) removeSelf() bool {
	for i, ww := range w.pool.workers {
		if ww.id == w.id {
			w.pool.workers = append(w.pool.workers[:i], w.pool.workers[i+1:]...)
			return true
		}
	}
	return false
}
