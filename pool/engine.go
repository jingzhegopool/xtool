package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is a user-registered function that processes a task.
type Handler func(ctx context.Context, task *Task) (any, error)

// TaskPool is the user-facing task pool with pluggable backends.
type TaskPool struct {
	cfg       Config
	backend   Backend
	handlers  map[string]Handler
	mu        sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	stats struct {
		tasksDone   atomic.Int64
		tasksFailed atomic.Int64
	}

	progress     map[string][2]int // taskID → [current, total]
	progressMu   sync.RWMutex

	onProgress      func(string, int, int)
	onComplete      func(*Task)
	onFailed        func(*Task, error)
	onBatchComplete func(string, []*TaskResult)

	started atomic.Bool
}

// New creates a new task pool with the given configuration.
// Backend tables (SQLite/MySQL) are auto-initialized on creation.
func New(cfg ...Config) (*TaskPool, error) {
	c := defaultConfig()
	if len(cfg) > 0 {
		c = applyConfig(c, cfg[0])
	}

	p := &TaskPool{
		cfg:      c,
		handlers: make(map[string]Handler),
		progress: make(map[string][2]int),
	}

	var err error
	p.backend, err = newBackend(c)
	if err != nil {
		return nil, err
	}

	if err := p.backend.Init(context.Background()); err != nil {
		return nil, err
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	// Start workers
	for i := 0; i < c.MaxWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}

	// Start batch checker
	if c.BatchCompleteCallback {
		p.wg.Add(1)
		go p.batchChecker()
	}

	p.started.Store(true)
	return p, nil
}

// Handle registers a handler for a task type.
func (p *TaskPool) Handle(typ string, handler Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[typ] = handler
}

// HandleFunc is a convenience wrapper for Handle.
func (p *TaskPool) HandleFunc(typ string, fn func(ctx context.Context, task *Task) (any, error)) {
	p.Handle(typ, fn)
}

// Submit enqueues a new task and returns its ID.
func (p *TaskPool) Submit(typ string, data any, opts ...SubmitOption) (string, error) {
	task := &Task{
		Type:       typ,
		Data:       marshalAny(data),
		CreatedAt:  time.Now(),
		MaxRetries: p.cfg.MaxRetries,
		Timeout:    p.cfg.DefaultTimeout,
		Priority:   0,
		Status:     StatusPending,
	}
	for _, opt := range opts {
		opt(task)
	}

	// Handle delayed tasks
	if !task.ScheduledAt.IsZero() {
		task.Status = StatusDelayed
	}

	if err := p.backend.Save(task); err != nil {
		return "", err
	}
	if err := p.backend.Enqueue(task); err != nil {
		return "", err
	}
	return task.ID, nil
}

// SubmitTask directly submits a pre-constructed Task.
func (p *TaskPool) SubmitTask(task *Task) error {
	if task.ID == "" {
		task.ID = newID()
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	if task.Status == StatusPending && !task.ScheduledAt.IsZero() {
		task.Status = StatusDelayed
	}
	if err := p.backend.Save(task); err != nil {
		return err
	}
	return p.backend.Enqueue(task)
}

// Stop gracefully shuts down the pool:
// - stops consuming new tasks
// - waits for running tasks to complete
// - closes the backend
func (p *TaskPool) Stop() {
	if !p.started.Load() {
		return
	}
	p.cancel()
	p.wg.Wait()
	p.backend.Close()
}

// Cancel removes a pending task by ID.
func (p *TaskPool) Cancel(id string) bool {
	return p.backend.Remove(id)
}

// CancelBatch cancels all pending tasks in a batch.
func (p *TaskPool) CancelBatch(batchID string) (int, error) {
	return p.backend.CancelBatch(batchID)
}

// Stats returns task count statistics by status.
func (p *TaskPool) Stats() (map[TaskStatus]int, error) {
	return p.backend.CountByStatus()
}

// Progress returns progress snapshots for all tasks.
// Key is task ID, value is [current, total].
func (p *TaskPool) Progress() map[string][2]int {
	p.progressMu.RLock()
	defer p.progressMu.RUnlock()
	result := make(map[string][2]int, len(p.progress))
	for k, v := range p.progress {
		result[k] = v
	}
	return result
}

// Tasks returns a paginated list of all tasks.
func (p *TaskPool) Tasks(limit, offset int) ([]*Task, error) {
	return p.backend.ListAll(limit, offset)
}

// TasksByStatus returns tasks filtered by status.
func (p *TaskPool) TasksByStatus(status TaskStatus, limit, offset int) ([]*Task, error) {
	return p.backend.ListByStatus(status, limit, offset)
}

// GetTask returns a single task by ID.
func (p *TaskPool) GetTask(id string) (*Task, error) {
	return p.backend.Get(id)
}

// DeleteTask removes a task by ID from storage.
func (p *TaskPool) DeleteTask(id string) error {
	return p.backend.Delete(id)
}

// Pause is not supported in v0.2.0. Reserved for future use.
func (p *TaskPool) Pause() {
	// no-op
}

// Resume is not supported in v0.2.0. Reserved for future use.
func (p *TaskPool) Resume() {
	// no-op
}

// Backend returns the underlying backend for direct access.
func (p *TaskPool) Backend() Backend {
	return p.backend
}

// --- Callbacks ---

// OnProgress registers a callback for task progress updates.
// Called when a task's current/total progress changes.
func (p *TaskPool) OnProgress(fn func(taskID string, current, total int)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onProgress = fn
}

// OnComplete registers a callback for successful task completion.
func (p *TaskPool) OnComplete(fn func(*Task)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onComplete = fn
}

// OnFailed registers a callback for task failure (final, no more retries).
func (p *TaskPool) OnFailed(fn func(*Task, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onFailed = fn
}

// OnBatchComplete registers a callback when all tasks in a batch finish.
func (p *TaskPool) OnBatchComplete(fn func(batchID string, results []*TaskResult)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onBatchComplete = fn
}

// --- Worker Loop ---

func (p *TaskPool) workerLoop() {
	defer p.wg.Done()

	for {
		task, err := p.backend.Dequeue(p.ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return
			}
			continue
		}

		p.executeTask(task)
	}
}

func (p *TaskPool) executeTask(task *Task) {
	p.mu.Lock()
	handler, ok := p.handlers[task.Type]
	// Grab callbacks under lock
	var onCompleteFn func(*Task)
	var onFailedFn func(*Task, error)
	if p.onComplete != nil {
		onCompleteFn = p.onComplete
	}
	if p.onFailed != nil {
		onFailedFn = p.onFailed
	}
	onBatch := p.onBatchComplete
	batchCfg := p.cfg.BatchCompleteCallback
	p.mu.Unlock()

	if !ok {
		p.backend.UpdateStatus(task.ID, StatusFailed, "unknown task type: "+task.Type)
		p.stats.tasksFailed.Add(1)
		if onFailedFn != nil {
			onFailedFn(task, ErrUnknownType)
		}
		p.checkBatchCompletion(task, onBatch, batchCfg)
		return
	}

	execCtx := p.ctx
	var cancel context.CancelFunc
	if task.Timeout > 0 {
		execCtx, cancel = context.WithTimeout(execCtx, task.Timeout)
		defer cancel()
	}

	var result any
	var execErr error

	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("pool: panic in handler: %v", r)
			}
		}()
		result, execErr = handler(execCtx, task)
	}()

	now := time.Now()
	task.DoneAt = &now

	if execErr != nil {
		if task.Retries < task.MaxRetries {
			task.Retries++
			task.Status = StatusPending
			task.Error = ""
			task.StartedAt = nil

			// Re-enqueue for retry
			if err := p.backend.Enqueue(task); err != nil {
				task.Status = StatusFailed
				task.Error = "retry: " + err.Error()
				p.backend.UpdateStatus(task.ID, StatusFailed, task.Error)
				p.stats.tasksFailed.Add(1)
				if onFailedFn != nil {
					onFailedFn(task, err)
				}
			}
			return
		}

		task.Status = StatusFailed
		task.Error = execErr.Error()
		p.backend.UpdateStatus(task.ID, StatusFailed, task.Error)
		p.stats.tasksFailed.Add(1)
		if onFailedFn != nil {
			onFailedFn(task, execErr)
		}
	} else {
		task.Status = StatusCompleted
		resultBytes := marshalAny(result)
		p.backend.UpdateResult(task.ID, []byte(resultBytes))
		p.backend.UpdateStatus(task.ID, StatusCompleted, "")
		p.stats.tasksDone.Add(1)
		if onCompleteFn != nil {
			task.Result = resultBytes
			onCompleteFn(task)
		}
	}

	p.checkBatchCompletion(task, onBatch, batchCfg)
}

// checkBatchCompletion checks if all tasks in the batch are done.
func (p *TaskPool) checkBatchCompletion(task *Task, callback func(string, []*TaskResult), enabled bool) {
	if !enabled || task.BatchID == "" || callback == nil {
		return
	}

	tasks, err := p.backend.ListByBatchID(task.BatchID)
	if err != nil {
		return
	}

	// Check if all tasks are in a terminal state
	allDone := len(tasks) > 0
	for _, t := range tasks {
		if t.Status != StatusCompleted && t.Status != StatusFailed && t.Status != StatusCancelled {
			allDone = false
			break
		}
	}

	if allDone {
		results := make([]*TaskResult, len(tasks))
		for i, t := range tasks {
			results[i] = &TaskResult{
				ID:     t.ID,
				Status: t.Status,
				Data:   t.Data,
				Result: t.Result,
				Error:  t.Error,
			}
		}
		callback(task.BatchID, results)
	}
}

// batchChecker periodically checks batch completion for memory backend
// (which doesn't have database-level triggers). Redundant but harmless
// for SQLite/MySQL.
func (p *TaskPool) batchChecker() {
	defer p.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.scanBatches()
		}
	}
}

func (p *TaskPool) scanBatches() {
	tasks, err := p.backend.ListAll(1000, 0)
	if err != nil {
		return
	}

	// Collect unique batch IDs that have at least one completed task
	// and no running tasks
	type batchState struct {
		allDone bool
		results []*TaskResult
		checked bool
	}
	batches := make(map[string]*batchState)

	for _, t := range tasks {
		if t.BatchID == "" {
			continue
		}
		if _, ok := batches[t.BatchID]; !ok {
			batches[t.BatchID] = &batchState{allDone: true}
		}
		if t.Status == StatusPending || t.Status == StatusRunning || t.Status == StatusDelayed || t.Status == StatusRetrying {
			batches[t.BatchID].allDone = false
		}
	}

	for batchID, state := range batches {
		if !state.allDone {
			continue
		}

		batchTasks, err := p.backend.ListByBatchID(batchID)
		if err != nil || len(batchTasks) == 0 {
			continue
		}

		results := make([]*TaskResult, len(batchTasks))
		for i, t := range batchTasks {
			results[i] = &TaskResult{
				ID: t.ID, Status: t.Status,
				Data: t.Data, Result: t.Result, Error: t.Error,
			}
		}

		p.mu.Lock()
		fn := p.onBatchComplete
		p.mu.Unlock()

		if fn != nil {
			go fn(batchID, results)
		}
	}
}

// --- Submit options ---

type SubmitOption func(*Task)

func WithPriority(p int) SubmitOption {
	return func(t *Task) { t.Priority = p }
}

func WithTimeout(d time.Duration) SubmitOption {
	return func(t *Task) { t.Timeout = d }
}

func WithRetries(n int) SubmitOption {
	return func(t *Task) { t.MaxRetries = n }
}

func WithBatchID(id string) SubmitOption {
	return func(t *Task) { t.BatchID = id }
}

func WithDelay(d time.Duration) SubmitOption {
	return func(t *Task) {
		t.ScheduledAt = time.Now().Add(d)
		if t.Status == StatusPending {
			t.Status = StatusDelayed
		}
	}
}

func WithScheduleAt(tm time.Time) SubmitOption {
	return func(t *Task) {
		t.ScheduledAt = tm
		if t.Status == StatusPending {
			t.Status = StatusDelayed
		}
	}
}

// --- helpers ---

func marshalAny(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{"__error":"` + err.Error() + `"}`)
	}
	return json.RawMessage(b)
}
