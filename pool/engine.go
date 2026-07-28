package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Handler 是用户注册的处理任务的函数类型。
type Handler func(ctx context.Context, task *Task) (any, error)

// TaskPool 是面向用户的任务池，带可插拔的后端。
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

	progress     map[string][2]int // taskID => [current, total]
	progressMu   sync.RWMutex

	onProgress      func(string, int, int)
	onComplete      func(*Task)
	onFailed        func(*Task, error)
	onBatchComplete func(string, []*TaskResult)

	started atomic.Bool
}

// New 创建一个任务池，使用给定的配置。
// SQLite/MySQL 后端的数据库表在创建时自动初始化。
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

	// 启动工作协程
	for i := 0; i < c.MaxWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}

	// 启动批次完成检查协程
	if c.BatchCompleteCallback {
		p.wg.Add(1)
		go p.batchChecker()
	}

	p.started.Store(true)
	return p, nil
}

// Handle 注册一个任务类型对应的处理函数。
func (p *TaskPool) Handle(typ string, handler Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[typ] = handler
}

// HandleFunc 是 Handle 的便捷包装。
func (p *TaskPool) HandleFunc(typ string, fn func(ctx context.Context, task *Task) (any, error)) {
	p.Handle(typ, fn)
}

// Submit 提交一个新任务到队列，并返回其 ID。
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

	// 处理延迟任务
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

// SubmitTask 直接提交一个预先构造好的 Task。
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

// Stop 优雅地关闭任务池：
// - 停止消费新任务
// - 等待正在运行的任务完成
// - 关闭后端存储
func (p *TaskPool) Stop() {
	if !p.started.Load() {
		return
	}
	p.cancel()
	p.wg.Wait()
	p.backend.Close()
}

// Cancel 按 ID 取消一个待处理的任务。
func (p *TaskPool) Cancel(id string) bool {
	return p.backend.Remove(id)
}

// CancelBatch 取消指定批次中所有待处理的任务。
func (p *TaskPool) CancelBatch(batchID string) (int, error) {
	return p.backend.CancelBatch(batchID)
}

// Stats 返回按状态分组的任务数量统计。
func (p *TaskPool) Stats() (map[TaskStatus]int, error) {
	return p.backend.CountByStatus()
}

// Progress 返回所有任务的进度快照。
// Key 为任务 ID，value 为 [当前进度, 总进度]。
func (p *TaskPool) Progress() map[string][2]int {
	p.progressMu.RLock()
	defer p.progressMu.RUnlock()
	result := make(map[string][2]int, len(p.progress))
	for k, v := range p.progress {
		result[k] = v
	}
	return result
}

// Tasks 返回分页的任务列表。
func (p *TaskPool) Tasks(limit, offset int) ([]*Task, error) {
	return p.backend.ListAll(limit, offset)
}

// TasksByStatus 返回按状态筛选的任务列表。
func (p *TaskPool) TasksByStatus(status TaskStatus, limit, offset int) ([]*Task, error) {
	return p.backend.ListByStatus(status, limit, offset)
}

// GetTask 根据 ID 获取单个任务。
func (p *TaskPool) GetTask(id string) (*Task, error) {
	return p.backend.Get(id)
}

// DeleteTask 从存储中删除指定 ID 的任务。
func (p *TaskPool) DeleteTask(id string) error {
	return p.backend.Delete(id)
}

// Pause 在 v0.2.0 中不支持。保留以供后续版本使用。
func (p *TaskPool) Pause() {
	// 空操作
}

// Resume 在 v0.2.0 中不支持。保留以供后续版本使用。
func (p *TaskPool) Resume() {
	// 空操作
}

// Backend 返回底层的后端实例，供直接访问。
func (p *TaskPool) Backend() Backend {
	return p.backend
}

// ------- 回调注册 -------

// OnProgress 注册任务进度更新回调。
// 当任务当前进度/总进度变化时被调用。
func (p *TaskPool) OnProgress(fn func(taskID string, current, total int)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onProgress = fn
}

// OnComplete 注册任务成功完成的回调。
func (p *TaskPool) OnComplete(fn func(*Task)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onComplete = fn
}

// OnFailed 注册任务失败（最终失败，不再重试）的回调。
func (p *TaskPool) OnFailed(fn func(*Task, error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onFailed = fn
}

// OnBatchComplete 注册一个批次所有任务完成时的回调。
func (p *TaskPool) OnBatchComplete(fn func(batchID string, results []*TaskResult)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onBatchComplete = fn
}

// ------- 工作协程 -------

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
	// 在锁内获取回调引用
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
		p.backend.UpdateStatus(task.ID, StatusFailed, "未知的任务类型: "+task.Type)
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
				execErr = fmt.Errorf("pool: 处理函数发生 panic: %v", r)
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

			// 重新入队以重试
			if err := p.backend.Enqueue(task); err != nil {
				task.Status = StatusFailed
				task.Error = "重试失败: " + err.Error()
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

// checkBatchCompletion 检查批次中的所有任务是否都已完成。
func (p *TaskPool) checkBatchCompletion(task *Task, callback func(string, []*TaskResult), enabled bool) {
	if !enabled || task.BatchID == "" || callback == nil {
		return
	}

	tasks, err := p.backend.ListByBatchID(task.BatchID)
	if err != nil {
		return
	}

	// 检查所有任务是否都处于终态
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

// batchChecker 定期扫描批次完成状态。
// 对内存后端是必需的（没有数据库级触发器）。
// 对 SQLite/MySQL 是冗余但无害的。
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

// ------- 提交选项 -------

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

// ------- 辅助函数 -------

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
