package pool

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics 收集任务池的运行时指标。
// 所有字段均为线程安全的基础类型，不引入外部依赖。
type Metrics struct {
	tasksSubmitted   atomic.Int64
	tasksCompleted   atomic.Int64
	tasksFailed      atomic.Int64
	progressUpdates  atomic.Int64

	typeMu     sync.Mutex
	typeSubmit map[string]*int64
	typeDone   map[string]*int64
	typeFail   map[string]*int64
	typeRun    map[string]*int64

	latMu   sync.Mutex
	latSum  map[string]*atomic.Int64 // total nanoseconds per type
	latCnt  map[string]*atomic.Int64 // call count per type
}

func newMetrics() *Metrics {
	return &Metrics{
		typeSubmit: make(map[string]*int64),
		typeDone:   make(map[string]*int64),
		typeFail:   make(map[string]*int64),
		typeRun:    make(map[string]*int64),
		latSum:     make(map[string]*atomic.Int64),
		latCnt:     make(map[string]*atomic.Int64),
	}
}

func (m *Metrics) incSubmitted(typ string) {
	m.tasksSubmitted.Add(1)
	m.getOrCreateInt64(&m.typeMu, &m.typeSubmit, typ)
	atomic.AddInt64(m.typeSubmit[typ], 1)
}

func (m *Metrics) incCompleted(typ string) {
	m.tasksCompleted.Add(1)
	m.getOrCreateInt64(&m.typeMu, &m.typeDone, typ)
	atomic.AddInt64(m.typeDone[typ], 1)
}

func (m *Metrics) incFailed(typ string) {
	m.tasksFailed.Add(1)
	m.getOrCreateInt64(&m.typeMu, &m.typeFail, typ)
	atomic.AddInt64(m.typeFail[typ], 1)
}

func (m *Metrics) addRunning(typ string, delta int) {
	m.getOrCreateInt64(&m.typeMu, &m.typeRun, typ)
	atomic.AddInt64(m.typeRun[typ], int64(delta))
}

func (m *Metrics) addLatency(typ string, dur time.Duration) {
	m.latMu.Lock()
	sum, ok := m.latSum[typ]
	if !ok {
		sum = new(atomic.Int64)
		m.latSum[typ] = sum
	}
	cnt, ok2 := m.latCnt[typ]
	if !ok2 {
		cnt = new(atomic.Int64)
		m.latCnt[typ] = cnt
	}
	m.latMu.Unlock()
	sum.Add(int64(dur))
	cnt.Add(1)
}

func (m *Metrics) incProgressUpdates() {
	m.progressUpdates.Add(1)
}

func (m *Metrics) getOrCreateInt64(mu *sync.Mutex, mapp *map[string]*int64, key string) {
	mu.Lock()
	if _, ok := (*mapp)[key]; !ok {
		var v int64
		(*mapp)[key] = &v
	}
	mu.Unlock()
}

// MetricsSnapshot 是 Metrics 的快照，可在运行时安全读取。
type MetricsSnapshot struct {
	TasksSubmitted   int64                `json:"tasks_submitted"`
	TasksCompleted   int64                `json:"tasks_completed"`
	TasksFailed      int64                `json:"tasks_failed"`
	ProgressUpdates  int64                `json:"progress_updates"`
	TypeStats        map[string]TypeStats `json:"type_stats"`
}

// TypeStats 按任务类型的统计信息。
type TypeStats struct {
	Submitted      int64 `json:"submitted"`
	Completed      int64 `json:"completed"`
	Failed         int64 `json:"failed"`
	Running        int64 `json:"running"`
	AvgLatencyMs   int64 `json:"avg_latency_ms"`
	TotalLatencyMs int64 `json:"total_latency_ms"`
}

// Snapshot 返回当前指标的原子快照。
func (m *Metrics) Snapshot() MetricsSnapshot {
	snap := MetricsSnapshot{
		TasksSubmitted:  m.tasksSubmitted.Load(),
		TasksCompleted:  m.tasksCompleted.Load(),
		TasksFailed:     m.tasksFailed.Load(),
		ProgressUpdates: m.progressUpdates.Load(),
		TypeStats:       make(map[string]TypeStats),
	}

	m.typeMu.Lock()
	types := make(map[string]struct{})
	for k := range m.typeSubmit { types[k] = struct{}{} }
	for k := range m.typeDone   { types[k] = struct{}{} }
	for k := range m.typeFail   { types[k] = struct{}{} }
	for k := range m.typeRun    { types[k] = struct{}{} }
	m.typeMu.Unlock()

	for t := range types {
		submit := m.safeLoad(&m.typeMu, &m.typeSubmit, t)
		done   := m.safeLoad(&m.typeMu, &m.typeDone, t)
		fail   := m.safeLoad(&m.typeMu, &m.typeFail, t)
		run    := m.safeLoad(&m.typeMu, &m.typeRun, t)

		avgMs := int64(0)
		totalMs := int64(0)
		m.latMu.Lock()
		sum, hasSum := m.latSum[t]
		cnt, hasCnt := m.latCnt[t]
		m.latMu.Unlock()
		if hasSum && hasCnt {
			s := sum.Load()
			c := cnt.Load()
			if c > 0 {
				totalMs = s / int64(time.Millisecond)
				avgMs = totalMs / c
			}
		}

		snap.TypeStats[t] = TypeStats{
			Submitted:      submit,
			Completed:      done,
			Failed:         fail,
			Running:        run,
			AvgLatencyMs:   avgMs,
			TotalLatencyMs: totalMs,
		}
	}

	return snap
}

func (m *Metrics) safeLoad(mu *sync.Mutex, mapp *map[string]*int64, key string) int64 {
	mu.Lock()
	defer mu.Unlock()
	if p, ok := (*mapp)[key]; ok {
		return atomic.LoadInt64(p)
	}
	return 0
}
