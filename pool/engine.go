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

	// 运行中任务的取消注册表 + 干预信号（Stop/Terminate 用）
	controlMu      sync.Mutex
	runningCancels map[string]context.CancelFunc // id => cancel()，仅运行中任务存在
	stopSignals    map[string]TaskStatus         // id => 期望终态（StatusPaused / StatusCancelled）

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
		runningCancels:    make(map[string]context.CancelFunc),
		stopSignals:       make(map[string]TaskStatus),
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
		runningCancels:    make(map[string]context.CancelFunc),
		stopSignals:       make(map[string]TaskStatus),
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

// Cancel 取消一个任务（兼容旧版，返回是否成功取消）。
// 排队中的任务（pending/delayed）直接标记为 cancelled；
// 运行中的任务发送取消信号，handler 响应 ctx.Done() 后终态为 cancelled。
// 旧版仅支持排队中任务，本版升级为全状态支持。
func (p *TaskPool) Cancel(id string) bool {
	return p.Terminate(id) == nil
}

// Terminate 终止一个任务，终态为 cancelled（不可自动恢复，但可 Start 重新激活）。
// - 运行中：发送取消信号，handler 退出后写入 cancelled
// - 排队中（pending/delayed）：直接标记为 cancelled
// - 已完成/已失败/已暂停/已取消：返回错误（幂等调用可返回 ErrTaskNotRunning 之外的状态错误）
func (p *TaskPool) Terminate(id string) error {
	p.controlMu.Lock()
	cancel, ok := p.runningCancels[id]
	if ok {
		p.stopSignals[id] = StatusCancelled
		p.controlMu.Unlock()
		cancel()
		poolLogger().Info("任务终止信号已发送", slog.String("id", id))
		return nil
	}
	p.controlMu.Unlock()

	if p.backend.Remove(id) {
		poolLogger().Info("任务已终止（排队中）", slog.String("id", id))
		return nil
	}

	// 已停止的任务（paused/failed）：直接置为 cancelled（补刀，保证"终止按钮永远可用"）
	task, err := p.backend.Get(id)
	if err != nil {
		return err
	}
	switch task.Status {
	case StatusPaused, StatusFailed:
		now := time.Now()
		task.DoneAt = &now
		if err := p.backend.UpdateStatus(id, StatusCancelled, "任务已终止"); err != nil {
			return err
		}
		poolLogger().Info("任务已终止（已停止任务）", slog.String("id", id))
		return nil
	case StatusCompleted, StatusCancelled:
		return fmt.Errorf("pool: 任务状态 %s 不可终止", task.Status)
	default:
		return fmt.Errorf("pool: 终止任务失败: 状态 %s", task.Status)
	}
}

// StopTask 停止一个运行中的任务（暂停），保留进度和 metadata，可通过 Continue 恢复。
// 停止语义（排空）：已经进入执行的小任务继续执行完成，但不再启动新的小任务——
// 这依赖 handler 在“小任务之间”检查 ctx.Done()（见设计文档 §8）。
// 若 handler 不响应 ctx.Done()，任务会继续运行到结束，但最终状态仍记为 paused，结果不落库。
// 非运行中任务返回 ErrTaskNotRunning。
func (p *TaskPool) StopTask(id string) error {
	p.controlMu.Lock()
	cancel, ok := p.runningCancels[id]
	if ok {
		p.stopSignals[id] = StatusPaused
		p.controlMu.Unlock()
		cancel()
		poolLogger().Info("任务停止信号已发送", slog.String("id", id))
		return nil
	}
	p.controlMu.Unlock()
	return ErrTaskNotRunning
}

// Start 重新激活一个非活动任务，使其重新入队执行：
// - delayed：立即执行（清除调度时间，改变主意场景）
// - paused：继续执行（保留进度、metadata、重试计数）
// - failed / cancelled：重新运行（重试计数清零）
// 其他状态返回 ErrTaskNotStartable。
func (p *TaskPool) Start(id string) error {
	task, err := p.backend.Get(id)
	if err != nil {
		return err
	}
	switch task.Status {
	case StatusDelayed, StatusPaused, StatusFailed, StatusCancelled:
	default:
		return ErrTaskNotStartable
	}

	if task.Status != StatusPaused {
		task.Retries = 0 // 重新运行视为全新尝试
	}
	task.Status = StatusPending
	task.ScheduledAt = time.Time{} // 延迟任务立即执行，清除调度时间
	task.StartedAt = nil
	task.DoneAt = nil
	task.Error = ""
	if err := p.backend.Enqueue(task); err != nil {
		return err
	}
	poolLogger().Info("任务已重新激活",
		slog.String("id", id),
		slog.String("type", task.Type),
	)
	return nil
}

// Continue 继续执行一个已暂停的任务，等价于 Start，语义更明确。
func (p *TaskPool) Continue(id string) error {
	return p.Start(id)
}

// Remove 移除一个任务（从存储中彻底删除）。
// 仅允许移除非运行中的任务：pending/delayed/paused/failed/cancelled/completed。
// 运行中的任务必须先 StopTask（等变 paused）或 Terminate（等变 cancelled）后才能移除，
// 对运行中任务调用返回 ErrTaskNotRemovable。
func (p *TaskPool) Remove(id string) error {
	task, err := p.backend.Get(id)
	if err != nil {
		return err
	}
	if task.Status == StatusRunning {
		return ErrTaskNotRemovable
	}
	if err := p.backend.Delete(id); err != nil {
		return err
	}
	poolLogger().Info("任务已移除", slog.String("id", id))
	return nil
}

// DeleteTask 从存储中删除指定 ID 的任务（兼容旧版，等价于 Remove）。
func (p *TaskPool) DeleteTask(id string) error {
	return p.Remove(id)
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

// Pause 在 v0.2.0 中不支持。保留以供后续版本使用。
// 注意：池级暂停（全局暂停所有任务）尚未实现；单任务暂停请使用 Stop(id)。
func (p *TaskPool) Pause() {
	// 空操作
}

// Resume 在 v0.2.0 中不支持。保留以供后续版本使用。
// 注意：池级恢复尚未实现；单任务恢复请使用 Continue(id) 或 Start(id)。
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

	// ===== 任务级取消上下文：注册到取消注册表，供 Stop/Terminate 查找 =====
	execCtx, taskCancel := context.WithCancel(p.ctx)
	p.controlMu.Lock()
	p.runningCancels[task.ID] = taskCancel
	p.controlMu.Unlock()
	defer func() {
		p.controlMu.Lock()
		delete(p.runningCancels, task.ID)
		p.controlMu.Unlock()
		taskCancel()
	}()

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

	// 超时叠加在任务级取消之上（超时或手动取消任一触发，handler 都会收到 Done）
	var timeoutCancel context.CancelFunc
	if task.Timeout > 0 {
		execCtx, timeoutCancel = context.WithTimeout(execCtx, task.Timeout)
	}
	if timeoutCancel != nil {
		defer timeoutCancel()
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

	// ===== 干预检查：必须在重试/失败/成功分支之前 =====
	// 若 Stop/Terminate 发过信号，handler 返回后直接写期望终态，
	// 否则 context.Canceled 会被当成普通失败触发重试。
	p.controlMu.Lock()
	desired, intervened := p.stopSignals[task.ID]
	if intervened {
		delete(p.stopSignals, task.ID)
	}
	p.controlMu.Unlock()

	if intervened {
		now := time.Now()
		task.DoneAt = &now
		if desired == StatusPaused {
			// 强制落库最新进度，保证 Continue 后续跑不丢进度
			p.progressMu.RLock()
			entry, hasProg := p.progress[task.ID]
			var cur, total int
			if hasProg {
				cur, total = entry.current, entry.total
			}
			p.progressMu.RUnlock()
			if hasProg {
				_ = p.backend.UpdateProgress(task.ID, cur, total)
				task.ProgressCurrent, task.ProgressTotal = cur, total
			}
			task.Status = StatusPaused
			_ = p.backend.UpdateStatus(task.ID, StatusPaused, "")
			poolLogger().Info("任务已暂停", slog.String("id", task.ID))
		} else {
			task.Status = StatusCancelled
			_ = p.backend.UpdateStatus(task.ID, StatusCancelled, "任务已终止")
			poolLogger().Info("任务已终止", slog.String("id", task.ID))
		}
		p.checkBatchCompletion(task)
		return
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
