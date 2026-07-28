package pool

import (
	"sync/atomic"
)

// poolStats is a lock-free statistics collector using atomic operations.
type poolStats struct {
	workersActive atomic.Int32
	workersIdle   atomic.Int32
	tasksQueued   atomic.Int64
	tasksDone     atomic.Int64
	tasksFailed   atomic.Int64
	tasksRunning  atomic.Int64
}

func (s *poolStats) incDone()    { s.tasksDone.Add(1) }
func (s *poolStats) incFailed()  { s.tasksFailed.Add(1) }
func (s *poolStats) incRunning() { s.tasksRunning.Add(1) }
func (s *poolStats) decRunning() { s.tasksRunning.Add(-1) }
func (s *poolStats) incActive()  { s.workersActive.Add(1) }
func (s *poolStats) decActive()  { s.workersActive.Add(-1) }
func (s *poolStats) incIdle()    { s.workersIdle.Add(1) }
func (s *poolStats) decIdle()    { s.workersIdle.Add(-1) }

// PoolStats is an immutable snapshot returned by Pool.Stats().
type PoolStats struct {
	WorkersActive int32 `json:"workers_active"`
	WorkersTotal  int32 `json:"workers_total"`
	TasksQueued   int   `json:"tasks_queued"`
	TasksRunning  int64 `json:"tasks_running"`
	TasksDone     int64 `json:"tasks_done"`
	TasksFailed   int64 `json:"tasks_failed"`
	Paused        bool  `json:"paused"`
}
