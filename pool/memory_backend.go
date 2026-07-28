package pool

import (
	"container/heap"
	"context"
	"encoding/json"
	"sync"
	"time"
)

// memoryBackend 将任务存储在内存中的 map + 优先级队列中。
type memoryBackend struct {
	mu       sync.Mutex
	tasks    map[string]*Task
	items    memHeapItems
	cap      int
	closed   bool
	wait     chan struct{} // 入队信号，用于阻塞 Dequeue
	progress map[string][2]int // id => [current, total]
}

func newMemoryBackend(cfg Config) (Backend, error) {
	cap := cfg.MaxQueueSize
	if cap <= 0 {
		cap = 100000
	}
	return &memoryBackend{
		tasks:    make(map[string]*Task),
		items:    make(memHeapItems, 0, 64),
		cap:      cap,
		wait:     make(chan struct{}),
		progress: make(map[string][2]int),
	}, nil
}

func init() {
	registerBackend("memory", newMemoryBackend)
}

func (m *memoryBackend) Init(ctx context.Context) error { return nil }

func (m *memoryBackend) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	close(m.wait)
	return nil
}

func (m *memoryBackend) Save(task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task.ID == "" {
		task.ID = newID()
	}
	m.tasks[task.ID] = task
	return nil
}

func (m *memoryBackend) Get(id string) (*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return t, nil
}

func (m *memoryBackend) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, id)
	delete(m.progress, id)
	return nil
}

func (m *memoryBackend) Enqueue(task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return ErrQueueClosed
	}
	if len(m.items) >= m.cap {
		return ErrQueueFull
	}

	m.tasks[task.ID] = task

	// 始终推入堆；Dequeue 会检查 ScheduledAt 和状态
	heap.Push(&m.items, &memItem{
		id:       task.ID,
		priority: task.Priority,
		created:  task.CreatedAt,
	})
	close(m.wait)
	m.wait = make(chan struct{})
	return nil
}

// Dequeue 阻塞等待直到有可用的待处理任务。
func (m *memoryBackend) Dequeue(ctx context.Context) (*Task, error) {
	return m.dequeue(ctx, 0)
}

func (m *memoryBackend) DequeueTimeout(ctx context.Context, timeout time.Duration) (*Task, error) {
	return m.dequeue(ctx, timeout)
}

func (m *memoryBackend) dequeue(ctx context.Context, timeout time.Duration) (*Task, error) {
	m.mu.Lock()

	for {
		// 找到第一个准备就绪的待处理任务（ScheduledAt <= 当前时间）
		now := time.Now()
		idx := -1
		var earliestDelay time.Time
		for i, item := range m.items {
			task := m.tasks[item.id]
			if task == nil {
				continue
			}
			// 同时检查 pending 和 delayed 状态（延迟任务在 ScheduledAt 到达后变为 pending）
			if task.Status == StatusPending || task.Status == StatusDelayed {
				if task.ScheduledAt.IsZero() || task.ScheduledAt.Before(now) {
					idx = i
					break
				}
				// 记录最早的延迟任务，用于休眠等待
				if earliestDelay.IsZero() || task.ScheduledAt.Before(earliestDelay) {
					earliestDelay = task.ScheduledAt
				}
			}
		}

		if idx >= 0 {
			item := heap.Remove(&m.items, idx).(*memItem)
			task := m.tasks[item.id]
			task.Status = StatusRunning
			now2 := time.Now()
			task.StartedAt = &now2
			m.mu.Unlock()
			return task, nil
		}

		if m.closed {
			m.mu.Unlock()
			return nil, ErrQueueClosed
		}

		// 计算需要休眠到最早延迟任务的时间
		var sleepDuration time.Duration
		if !earliestDelay.IsZero() {
			sleepDuration = time.Until(earliestDelay)
			if sleepDuration < 0 {
				sleepDuration = 0
			}
		}

		wait := m.wait
		m.mu.Unlock()

		var timeoutCh <-chan time.Time
		if timeout > 0 {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			timeoutCh = timer.C
		}

		select {
		case <-wait:
			// 有新任务入队，重新检查
		case <-timeoutCh:
			return nil, context.DeadlineExceeded
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleepDuration):
			// 因延迟任务应已就绪而唤醒
		}

		m.mu.Lock()
	}
}

func (m *memoryBackend) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, task := range m.tasks {
		if task.Status == StatusPending {
			if task.ScheduledAt.IsZero() || task.ScheduledAt.Before(time.Now()) {
				count++
			}
		}
	}
	return count
}

func (m *memoryBackend) Remove(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[id]
	if !ok {
		return false
	}
	if task.Status != StatusPending && task.Status != StatusDelayed {
		return false
	}

	// 从堆中移除
	for i, item := range m.items {
		if item.id == id {
			heap.Remove(&m.items, i)
			break
		}
	}

	task.Status = StatusCancelled
	return true
}

func (m *memoryBackend) UpdateStatus(id string, status TaskStatus, errStr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.tasks[id]
	if !ok {
		return ErrTaskNotFound
	}
	task.Status = status
	task.Error = errStr
	if status == StatusCompleted || status == StatusFailed || status == StatusCancelled {
		now := time.Now()
		task.DoneAt = &now
	}
	return nil
}

func (m *memoryBackend) UpdateProgress(id string, current, total int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.progress[id] = [2]int{current, total}
	if t, ok := m.tasks[id]; ok {
		t.ProgressCurrent = current
		t.ProgressTotal = total
	}
	return nil
}

func (m *memoryBackend) UpdateResult(id string, result []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tasks[id]; ok {
		t.Result = result
	}
	return nil
}

func (m *memoryBackend) ListByBatchID(batchID string) ([]*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*Task
	for _, t := range m.tasks {
		if t.BatchID == batchID {
			result = append(result, t)
		}
	}
	return result, nil
}

func (m *memoryBackend) ListByStatus(status TaskStatus, limit, offset int) ([]*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*Task
	for _, t := range m.tasks {
		if t.Status == status {
			result = append(result, t)
		}
	}
	// 简单分页（不保证排序）
	if offset >= len(result) {
		return nil, nil
	}
	if limit <= 0 {
		limit = len(result)
	}
	end := offset + limit
	if end > len(result) {
		end = len(result)
	}
	return result[offset:end], nil
}

func (m *memoryBackend) ListAll(limit, offset int) ([]*Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*Task
	for _, t := range m.tasks {
		result = append(result, t)
	}
	if offset >= len(result) {
		return nil, nil
	}
	if limit <= 0 {
		limit = len(result)
	}
	end := offset + limit
	if end > len(result) {
		end = len(result)
	}
	return result[offset:end], nil
}

func (m *memoryBackend) CountByStatus() (map[TaskStatus]int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := map[TaskStatus]int{
		StatusPending:   0,
		StatusDelayed:   0,
		StatusRunning:   0,
		StatusCompleted: 0,
		StatusFailed:    0,
		StatusCancelled: 0,
		StatusRetrying:  0,
	}
	for _, t := range m.tasks {
		counts[t.Status]++
	}
	return counts, nil
}

func (m *memoryBackend) CancelBatch(batchID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cancelled := 0
	for _, t := range m.tasks {
		if t.BatchID == batchID && (t.Status == StatusPending || t.Status == StatusDelayed) {
			t.Status = StatusCancelled
			cancelled++
		}
	}
	// 清理堆
	var remaining memHeapItems
	for _, item := range m.items {
		t := m.tasks[item.id]
		if t == nil || t.Status == StatusPending || t.Status == StatusDelayed {
			continue
		}
		remaining = append(remaining, item)
	}
	heap.Init(&remaining)
	m.items = remaining
	return cancelled, nil
}

// ------- 堆元素 -------

// memItem 是内存优先队列堆中的元素。
type memItem struct {
	id       string
	priority int
	created  time.Time
}

// memHeapItems 实现 heap.Interface 接口。
type memHeapItems []*memItem

func (h memHeapItems) Len() int { return len(h) }

func (h memHeapItems) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority
	}
	return h[i].created.Before(h[j].created)
}

func (h memHeapItems) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *memHeapItems) Push(x any) {
	*h = append(*h, x.(*memItem))
}

func (h *memHeapItems) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}


// Recover 内存后端不需要恢复，数据已在进程重启时丢失。
func (m *memoryBackend) Recover(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 将 running 任务重置为 pending
	for _, t := range m.tasks {
		if t.Status == StatusRunning || t.Status == StatusRetrying {
			t.Status = StatusPending
			t.StartedAt = nil
			t.Error = ""
		}
	}
	return nil
}

// UpdateMetadata 更新任务的 metadata。
func (m *memoryBackend) UpdateMetadata(id string, metadata json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tasks[id]; ok {
		t.Metadata = metadata
	}
	return nil
}



