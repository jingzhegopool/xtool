package pool

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the user-registered function for processing a task.
// ctx carries timeout and cancellation; job contains the task data.
// The return value is serialized into job.Result.
type Handler func(ctx context.Context, job *Job) (any, error)

// pool is the core task pool engine.
type pool struct {
	cfg      Config
	queue    *priorityQueue
	handlers map[string]Handler
	mu       sync.Mutex

	workers    []*worker
	workersIdx int

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	paused atomic.Bool

	stats *poolStats

	onComplete func(*Job)
	onFailed   func(*Job, error)
}

func newPool(cfg Config) *pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &pool{
		cfg:      cfg,
		queue:    newPriorityQueue(cfg.QueueCap),
		handlers: make(map[string]Handler),
		ctx:      ctx,
		cancel:   cancel,
		stats:    &poolStats{},
	}
}

// start launches the minimum number of workers and the scaler goroutine.
func (p *pool) start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.cfg.MinWorkers; i++ {
		p.spawnWorkerLocked()
	}
	go p.scaler()
}

// spawnWorkerLocked creates and starts a worker. Caller must hold p.mu.
func (p *pool) spawnWorkerLocked() {
	w := &worker{
		id:   p.workersIdx,
		pool: p,
	}
	p.workersIdx++
	p.workers = append(p.workers, w)
	p.wg.Add(1)
	p.stats.incIdle()
	go w.run()
}

// stop gracefully stops the pool: cancel context, close queue, wait for workers.
func (p *pool) stop() {
	p.cancel() // context.CancelFunc
	p.queue.Close()
	p.wg.Wait()
}

// pause suspends dequeuing new tasks. Running tasks continue to completion.
func (p *pool) pause() {
	p.paused.Store(true)
}

// resume resumes dequeuing tasks from the queue.
func (p *pool) resume() {
	p.paused.Store(false)
}

// submit creates and enqueues a new job.
func (p *pool) submit(job *Job) error {
	if job.ID == "" {
		job.ID = newID()
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}
	if job.Timeout <= 0 {
		job.Timeout = p.cfg.DefaultTimeout
	}
	if job.MaxRetries <= 0 {
		job.MaxRetries = p.cfg.DefaultRetries
	}
	job.Status = StatusPending
	return p.queue.Enqueue(job)
}

// cancelJob removes a pending job from the queue by ID.
// Running jobs cannot be cancelled. Returns true if removed.
func (p *pool) cancelJob(id string) bool {
	return p.queue.Remove(id)
}

// statsSnapshot returns the current pool state as an immutable snapshot.
func (p *pool) statsSnapshot() PoolStats {
	p.mu.Lock()
	total := int32(len(p.workers))
	p.mu.Unlock()

	return PoolStats{
		WorkersActive: p.stats.workersActive.Load(),
		WorkersTotal:  total,
		TasksQueued:   p.queue.Pending(),
		TasksRunning:  p.stats.tasksRunning.Load(),
		TasksDone:     p.stats.tasksDone.Load(),
		TasksFailed:   p.stats.tasksFailed.Load(),
		Paused:        p.paused.Load(),
	}
}

// scaler periodically checks queue depth and adds workers if needed.
func (p *pool) scaler() {
	ticker := time.NewTicker(p.cfg.ScalerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.scaleUpIfNeeded()
		}
	}
}

func (p *pool) scaleUpIfNeeded() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.paused.Load() {
		return
	}

	pending := p.queue.Pending()
	active := len(p.workers)

	// Scale up when queue depth exceeds 2x active workers and below max
	if pending > active*2 && active < p.cfg.MaxWorkers {
		addN := (pending - active*2) / 10
		if addN < 1 {
			addN = 1
		}
		maxAdd := p.cfg.MaxWorkers - active
		if addN > maxAdd {
			addN = maxAdd
		}
		for i := 0; i < addN; i++ {
			p.spawnWorkerLocked()
		}
	}
}

// --- helpers ---

func (p *pool) fireOnComplete(job *Job) {
	if p.onComplete != nil {
		p.onComplete(job)
	}
}

func (p *pool) fireOnFailed(job *Job, err error) {
	if p.onFailed != nil {
		p.onFailed(job, err)
	}
}

func (p *pool) getHandler(typ string) (Handler, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.handlers[typ]
	return h, ok
}

func (p *pool) registerHandler(typ string, h Handler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if h == nil {
		delete(p.handlers, typ)
	} else {
		p.handlers[typ] = h
	}
}

// checkDone non-blockingly checks if the context is done.
func checkDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// marshalResult serializes the handler return value to json.RawMessage.
func marshalResult(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		log.Printf("pool: marshal result error: %v", err)
		return nil
	}
	return json.RawMessage(b)
}
