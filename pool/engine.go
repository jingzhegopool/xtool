package pool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Handler 是用户注册的处理任务的函数类型。
type Handler func(ctx context.Context, task *Task) (any, error)

// HandlerOption 配置 Handler 注册时的选项。
type HandlerOption struct {
	apply func(*handlerConfig)
}

type handlerConfig struct {
	concurrency int // 0 = 不限制
}

// WithConcurrency 限制该任务类型的最大并发执行数。
// 例如 WithConcurrency(2) 表示同一时刻最多 2 个该类型的任务在跑。
// 0 或不设置表示不限制（共享全局 MaxWorkers）。
func WithConcurrency(n int) HandlerOption {
	if n <= 0 {
		n = 0
	}
	return HandlerOption{apply: func(c *handlerConfig) {
		c.concurrency = n
	}}
}

// batchCounter 跟踪一个批次的任务完成情况。
type batchCounter struct {
	mu    sync.Mutex
	total int
	done  int
}

// TaskPool 是面向用户的任务池，带可插拔的后端。
type TaskPool struct {
	cfg      Config
	backend  Backend
	handlers map[string]Handler
	mu       sync.Mutex

	// 类型级并发控制
	handlerSemaphores map[string]chan struct{}

	// 批次计数器
	batchCounters map[string]*batchCounter
	batchMu       sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	stats struct {
		tasksDone   atomic.Int64
		tasksFailed atomic.Int64
	}

	progress   map[string]*progressEntry
	progressMu sync.RWMutex

	onStart         func(*Task)
	onProgress      func(string, int, int)
	onComplete      func(*Task)
	onFailed        func(*Task, error)
	onBatchComplete func(string, []*TaskResult)

	started atomic.Bool
	metrics *Metrics
}

// New 创建一个任务池，使用给定的配置。
// SQLite/MySQL 后端的数据库表在创建时自动初始化。
func New(cfg ...Config) (*TaskPool, error) {
	c := defaultConfig()
	if len(cfg) > 0 {
		c = applyConfig(c, cfg[0])
	}

	p := &TaskPool{
		cfg:               c,
		handlers:          make(map[string]Handler),
		handlerSemaphores: make(map[string]chan struct{}),
		progress:          make(map[string]*progressEntry),
		batchCounters:     make(map[string]*batchCounter),
		metrics:           newMetrics(),
	}

	var err error
	p.backend, err = newBackend(c)
	if err != nil {
		return nil, err
	}

	if err := p.init(); err != nil {
		return nil, err
	}

	poolLogger().Info("任务池已创建",
		slog.String("backend", c.Backend),
		slog.Int("max_workers", c.MaxWorkers),
		slog.String("mode", c.Mode),
	)
	return p, nil
}

// NewWithBackend 使用给定的 Backend 实例创建任务池，不经过配置中的 Backend 选择逻辑。
// 适用于需要注入自定义后端的场景（如非标准数据库、已有连接、适配器等）。
func NewWithBackend(b Backend, cfg ...Config) (*TaskPool, error) {
	c := defaultConfig()
	if len(cfg) > 0 {
		c = applyConfig(c, cfg[0])
	}

	p := &TaskPool{
		cfg:               c,
		backend:           b,
		handlers:          make(map[string]Handler),
		handlerSemaphores: make(map[string]chan struct{}),
		progress:          make(map[string]*progressEntry),
		batchCounters:     make(map[string]*batchCounter),
		metrics:           newMetrics(),
	}

	if err := p.init(); err != nil {
		return nil, err
	}

	poolLogger().Info("任务池已创建（自定义后端）",
		slog.Int("max_workers", c.MaxWorkers),
		slog.String("mode", c.Mode),
	)
	return p, nil
}

// init 执行公共初始化逻辑。
func (p *TaskPool) init() error {
	if err := p.backend.Init(context.Background()); err != nil {
		return err
	}

	// 启动时恢复：重置上次崩溃遗留的 Running 任务，保留 metadata 和进度数据
	if err := p.backend.Recover(context.Background()); err != nil {
		return fmt.Errorf("pool: 恢复失败: %w", err)
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	// 启动工作协程
	for i := 0; i < p.cfg.MaxWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop()
	}

	// 注意：batchChecker 已移除，改用计数器追踪（batchCounters）
	// 任务完成时在 executeTask 末尾触发 checkBatchCompletion。

	p.started.Store(true)
	poolLogger().Debug("任务池工作协程已启动",
		slog.Int("workers", p.cfg.MaxWorkers),
	)
	return nil
}

// Handle 注册一个任务类型对应的处理函数。
// 可选传入 HandlerOption，如 WithConcurrency(n) 限制该类型的并发数。
func (p *TaskPool) Handle(typ string, handler Handler, opts ...HandlerOption) {
	cfg := handlerConfig{}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[typ] = handler

	if cfg.concurrency > 0 {
		p.handlerSemaphores[typ] = make(chan struct{}, cfg.concurrency)
		poolLogger().Debug("注册任务处理器（限流）",
			slog.String("type", typ),
			slog.Int("concurrency", cfg.concurrency),
		)
	} else {
		delete(p.handlerSemaphores, typ)
		poolLogger().Debug("注册任务处理器", slog.String("type", typ))
	}
}

// HandleFunc 是 Handle 的便捷包装。
func (p *TaskPool) HandleFunc(typ string, fn func(ctx context.Context, task *Task) (any, error), opts ...HandlerOption) {
	p.Handle(typ, fn, opts...)
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
		poolLogger().Error("任务入队失败",
			slog.String("id", task.ID),
			slog.String("type", task.Type),
			slog.String("error", err.Error()),
		)
		return "", err
	}

	if p.metrics != nil {
		p.metrics.incSubmitted(typ)
		p.trackBatchSubmit(task)
	}

	poolLogger().Info("任务已提交",
		slog.String("id", task.ID),
		slog.String("type", task.Type),
		slog.String("status", task.Status.String()),
	)
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
		poolLogger().Error("保存任务失败",
			slog.String("id", task.ID),
			slog.String("type", task.Type),
			slog.String("error", err.Error()),
		)
		return err
	}
	if err := p.backend.Enqueue(task); err != nil {
		poolLogger().Error("任务入队失败",
			slog.String("id", task.ID),
			slog.String("type", task.Type),
			slog.String("error", err.Error()),
		)
		return err
	}

	if p.metrics != nil {
		p.metrics.incSubmitted(task.Type)
		p.trackBatchSubmit(task)
	}

	poolLogger().Info("任务已提交",
		slog.String("id", task.ID),
		slog.String("type", task.Type),
		slog.String("status", task.Status.String()),
	)
	return nil
}

// trackBatchSubmit 记录批次计数器。
func (p *TaskPool) trackBatchSubmit(task *Task) {
	if task.BatchID == "" || !p.cfg.BatchCompleteCallback {
		return
	}
	p.batchMu.Lock()
	bc, ok := p.batchCounters[task.BatchID]
	if !ok {
		bc = &batchCounter{total: 1}
		p.batchCounters[task.BatchID] = bc
	} else {
		bc.total++
	}
	p.batchMu.Unlock()
}

// Stop 优雅地关闭任务池：
// - 停止消费新任务
// - 等待正在运行的任务完成
// - 关闭后端存储
func (p *TaskPool) Stop() {
	if !p.started.Load() {
		return
	}
	poolLogger().Info("任务池正在停止")
	p.cancel()
	p.wg.Wait()
	if err := p.backend.Close(); err != nil {
		poolLogger().Error("后端关闭失败", slog.String("error", err.Error()))
	} else {
		poolLogger().Info("任务池已停止")
	}
}

// Cancel 按 ID 取消一个待处理的任务。
func (p *TaskPool) Cancel(id string) bool {
	ok := p.backend.Remove(id)
	if ok {
		poolLogger().Info("任务已取消", slog.String("id", id))
	} else {
		poolLogger().Warn("取消任务失败", slog.String("id", id))
	}
	return ok
}

// CancelBatch 取消指定批次中所有待处理的任务。
func (p *TaskPool) CancelBatch(batchID string) (int, error) {
	n, err := p.backend.CancelBatch(batchID)
	if err != nil {
		poolLogger().Error("取消批次失败",
			slog.String("batch_id", batchID),
			slog.String("error", err.Error()),
		)
		return n, err
	}
	poolLogger().Info("批次已取消",
		slog.String("batch_id", batchID),
		slog.Int("count", n),
	)
	return n, nil
}

// Stats 返回按状态分组的任务数量统计。
func (p *TaskPool) Stats() (map[TaskStatus]int, error) {
	return p.backend.CountByStatus()
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

// SaveTaskMetadata 更新任务的 metadata（用户自定义数据）。
// 可用于持久化 checkpoint 信息，支持崩溃后断点续做。
// 等价于 backend.UpdateMetadata()。
func (p *TaskPool) SaveTaskMetadata(id string, metadata json.RawMessage) error {
	return p.backend.UpdateMetadata(id, metadata)
}

// Metrics 返回当前运行时指标的原子快照。
// 可用于监控面板、调试端点或日志输出。
func (p *TaskPool) Metrics() MetricsSnapshot {
	if p.metrics == nil {
		return MetricsSnapshot{TypeStats: make(map[string]TypeStats)}
	}
	return p.metrics.Snapshot()
}

// ------- 回调注册 -------

// OnStart 注册任务开始执行时的回调。
func (p *TaskPool) OnStart(fn func(*Task)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStart = fn
}

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
	poolLogger().Debug("工作协程已启动")
	defer poolLogger().Debug("工作协程已退出")

	for {
		task, err := p.backend.Dequeue(p.ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return
			}
			poolLogger().Debug("出队失败", slog.String("error", err.Error()))
			continue
		}

		p.executeTask(task)
	}
}

func (p *TaskPool) executeTask(task *Task) {
	p.mu.Lock()
	handler, ok := p.handlers[task.Type]
	// 获取类型级信号量
	sem, hasSem := p.handlerSemaphores[task.Type]
	// 在锁内获取回调引用
	var onCompleteFn func(*Task)
	var onFailedFn func(*Task, error)
	var onStartFn func(*Task)
	if p.onStart != nil {
		onStartFn = p.onStart
	}
	if p.onComplete != nil {
		onCompleteFn = p.onComplete
	}
	if p.onFailed != nil {
		onFailedFn = p.onFailed
	}
	p.mu.Unlock()

	// 类型级并发控制：获取信号量
	if hasSem {
		select {
		case sem <- struct{}{}:
		case <-p.ctx.Done():
			return
		}
		defer func() { <-sem }()
	}

	if p.metrics != nil {
		p.metrics.addRunning(task.Type, 1)
		defer p.metrics.addRunning(task.Type, -1)
	}

	poolLogger().Debug("开始执行任务",
		slog.String("id", task.ID),
		slog.String("type", task.Type),
		slog.Int("retries", task.Retries),
		slog.Int("max_retries", task.MaxRetries),
	)

	if onStartFn != nil {
		onStartFn(task)
	}

	if !ok {
		poolLogger().Error("未知任务类型",
			slog.String("id", task.ID),
			slog.String("type", task.Type),
		)
		p.backend.UpdateStatus(task.ID, StatusFailed, "未知的任务类型: "+task.Type)
		p.stats.tasksFailed.Add(1)
		if p.metrics != nil {
			p.metrics.incFailed(task.Type)
		}
		if onFailedFn != nil {
			onFailedFn(task, ErrUnknownType)
		}
		p.checkBatchCompletion(task)
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
	startTime := time.Now()

	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("pool: 处理函数发生 panic: %v", r)
				poolLogger().Error("任务处理函数 panic",
					slog.String("id", task.ID),
					slog.String("type", task.Type),
					slog.Any("panic", r),
				)
			}
		}()
		result, execErr = handler(execCtx, task)
	}()

	elapsed := time.Since(startTime)
	if p.metrics != nil {
		p.metrics.addLatency(task.Type, elapsed)
	}

	now := time.Now()
	task.DoneAt = &now

	if execErr != nil {
		if task.Retries < task.MaxRetries {
			task.Retries++
			task.Status = StatusPending
			task.Error = ""
			task.StartedAt = nil

			poolLogger().Warn("任务执行失败，准备重试",
				slog.String("id", task.ID),
				slog.String("type", task.Type),
				slog.Int("retries", task.Retries),
				slog.Int("max_retries", task.MaxRetries),
				slog.String("error", execErr.Error()),
			)

			// 重新入队以重试
			if err := p.backend.Enqueue(task); err != nil {
				task.Status = StatusFailed
				task.Error = "重试失败: " + err.Error()
				p.backend.UpdateStatus(task.ID, StatusFailed, task.Error)
				p.stats.tasksFailed.Add(1)
				if p.metrics != nil {
					p.metrics.incFailed(task.Type)
				}
				poolLogger().Error("任务重试入队失败",
					slog.String("id", task.ID),
					slog.String("type", task.Type),
					slog.String("error", err.Error()),
				)
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
		if p.metrics != nil {
			p.metrics.incFailed(task.Type)
		}
		poolLogger().Error("任务最终失败",
			slog.String("id", task.ID),
			slog.String("type", task.Type),
			slog.Int("retries", task.Retries),
			slog.String("error", execErr.Error()),
		)
		if onFailedFn != nil {
			onFailedFn(task, execErr)
		}
	} else {
		task.Status = StatusCompleted
		resultBytes := marshalAny(result)
		p.backend.UpdateResult(task.ID, []byte(resultBytes))
		p.backend.UpdateStatus(task.ID, StatusCompleted, "")
		p.stats.tasksDone.Add(1)
		if p.metrics != nil {
			p.metrics.incCompleted(task.Type)
		}
		poolLogger().Info("任务执行完成",
			slog.String("id", task.ID),
			slog.String("type", task.Type),
			slog.Duration("elapsed", elapsed),
		)
		if onCompleteFn != nil {
			task.Result = resultBytes
			onCompleteFn(task)
		}
	}

	p.checkBatchCompletion(task)
}

// checkBatchCompletion 检查批次中的所有任务是否都已完成。
// 使用计数器追踪，O(1) 复杂度，无需扫描数据库。
func (p *TaskPool) checkBatchCompletion(task *Task) {
	if !p.cfg.BatchCompleteCallback || task.BatchID == "" {
		return
	}

	p.mu.Lock()
	callback := p.onBatchComplete
	p.mu.Unlock()

	if callback == nil {
		return
	}

	// 计数器递减
	p.batchMu.Lock()
	bc, ok := p.batchCounters[task.BatchID]
	if !ok {
		p.batchMu.Unlock()
		return
	}
	bc.done++
	remaining := bc.total - bc.done
	p.batchMu.Unlock()

	if remaining > 0 {
		return
	}

	// 所有任务完成，获取结果并触发回调
	tasks, err := p.backend.ListByBatchID(task.BatchID)
	if err != nil {
		poolLogger().Error("查询批次任务失败",
			slog.String("batch_id", task.BatchID),
			slog.String("error", err.Error()),
		)
		return
	}
	if len(tasks) == 0 {
		return
	}

	poolLogger().Info("批次全部完成",
		slog.String("batch_id", task.BatchID),
		slog.Int("count", len(tasks)),
	)
	results := make([]*TaskResult, len(tasks))
	for i, t := range tasks {
		results[i] = &TaskResult{
			ID: t.ID, Status: t.Status,
			Data: t.Data, Result: t.Result, Error: t.Error,
		}
	}
	callback(task.BatchID, results)
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
