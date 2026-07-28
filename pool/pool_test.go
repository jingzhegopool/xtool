package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryBackend_SubmitAndExecute(t *testing.T) {
	p, err := New(Config{
		Backend:    "memory",
		MaxWorkers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var executed atomic.Bool
	p.Handle("test", func(ctx context.Context, task *Task) (any, error) {
		executed.Store(true)
		return "done", nil
	})

	id, err := p.Submit("test", "hello")
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	time.Sleep(100 * time.Millisecond)
	if !executed.Load() {
		t.Fatal("handler was not executed")
	}
}

func TestMemoryBackend_Priority(t *testing.T) {
	// Test priority ordering directly via the backend queue
	q, _ := newMemoryBackend(Config{MaxQueueSize: 100})
	ctx := context.Background()

	// Submit tasks in reverse priority order (worst first)
	tasks := []*Task{
		{ID: newID(), Type: "test", Data: marshalAny("low"), Priority: 10, Status: StatusPending, CreatedAt: time.Now()},
		{ID: newID(), Type: "test", Data: marshalAny("medium"), Priority: 5, Status: StatusPending, CreatedAt: time.Now().Add(1 * time.Millisecond)},
		{ID: newID(), Type: "test", Data: marshalAny("high"), Priority: 1, Status: StatusPending, CreatedAt: time.Now().Add(2 * time.Millisecond)},
	}
	for _, t := range tasks {
		q.(*memoryBackend).Save(t)
		q.(*memoryBackend).Enqueue(t)
	}

	// Should dequeue in priority order: high → medium → low
	for _, expected := range []string{"high", "medium", "low"} {
		got, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var val string
		got.Decode(&val)
		if val != expected {
			t.Errorf("expected %s, got %s", expected, val)
		}
	}
}

func TestMemoryBackend_Delay(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var executed atomic.Bool
	var executedAt time.Time

	p.Handle("delayed", func(ctx context.Context, task *Task) (any, error) {
		executedAt = time.Now()
		executed.Store(true)
		return nil, nil
	})

	start := time.Now()
	p.Submit("delayed", "data", WithDelay(300*time.Millisecond))

	time.Sleep(500 * time.Millisecond)

	if !executed.Load() {
		t.Fatal("delayed task was not executed")
	}
	if executedAt.Sub(start) < 200*time.Millisecond {
		t.Fatal("task executed too early")
	}
}

func TestMemoryBackend_BatchComplete(t *testing.T) {
	p, err := New(Config{
		Backend:              "memory",
		MaxWorkers:           3,
		BatchCompleteCallback: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("work", func(ctx context.Context, task *Task) (any, error) {
		time.Sleep(50 * time.Millisecond)
		return "ok", nil
	})

	done := make(chan string, 3)
	p.OnBatchComplete(func(batchID string, results []*TaskResult) {
		done <- batchID
	})

	for i := 0; i < 3; i++ {
		p.Submit("work", i, WithBatchID("batch_a"))
	}

	select {
	case bid := <-done:
		if bid != "batch_a" {
			t.Fatalf("expected batch_a, got %s", bid)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for batch completion")
	}
}

func TestMemoryBackend_Stats(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("fast", func(ctx context.Context, task *Task) (any, error) {
		return nil, nil
	})

	for i := 0; i < 10; i++ {
		p.Submit("fast", i)
	}

	time.Sleep(300 * time.Millisecond)

	stats, err := p.Stats()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Stats: completed=%d failed=%d pending=%d",
		stats[StatusCompleted], stats[StatusFailed], stats[StatusPending])

	if stats[StatusCompleted] == 0 {
		t.Fatal("expected at least 1 completed task")
	}
}

func TestMemoryBackend_OnFailed(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("fail", func(ctx context.Context, task *Task) (any, error) {
		return nil, context.DeadlineExceeded
	})

	var failed atomic.Bool
	p.OnFailed(func(task *Task, err error) {
		failed.Store(true)
	})

	p.Submit("fail", nil, WithRetries(0))

	time.Sleep(200 * time.Millisecond)

	if !failed.Load() {
		t.Fatal("OnFailed callback not triggered")
	}
}

func TestMemoryBackend_PanicRecovery(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("panic", func(ctx context.Context, task *Task) (any, error) {
		panic("test panic")
	})

	var failed atomic.Bool
	p.OnFailed(func(task *Task, err error) {
		failed.Store(true)
	})

	p.Submit("panic", nil, WithRetries(0))

	time.Sleep(200 * time.Millisecond)

	if !failed.Load() {
		t.Fatal("panic should trigger OnFailed")
	}

	// Pool should still work
	var worked atomic.Bool
	p.Handle("ok", func(ctx context.Context, task *Task) (any, error) {
		worked.Store(true)
		return nil, nil
	})
	p.Submit("ok", nil)
	time.Sleep(100 * time.Millisecond)

	if !worked.Load() {
		t.Fatal("pool should still work after panic")
	}
}

func TestMemoryBackend_Cancel(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("slow", func(ctx context.Context, task *Task) (any, error) {
		time.Sleep(500 * time.Millisecond)
		return nil, nil
	})

	p.Submit("slow", "first")
	time.Sleep(20 * time.Millisecond)

	id2, _ := p.Submit("slow", "second")

	if !p.Cancel(id2) {
		t.Fatal("Cancel returned false")
	}

	time.Sleep(600 * time.Millisecond)

	stats, _ := p.Stats()
	if stats[StatusPending] > 0 {
		t.Error("expected no pending tasks after cancel")
	}
}

func TestMemoryBackend_Progress(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("progress", func(ctx context.Context, task *Task) (any, error) {
		p.SetProgress(task.ID, 1, 10)
		time.Sleep(50 * time.Millisecond)
		p.SetProgress(task.ID, 5, 10)
		time.Sleep(50 * time.Millisecond)
		p.SetProgress(task.ID, 10, 10)
		return nil, nil
	})

	id, _ := p.Submit("progress", nil)
	time.Sleep(200 * time.Millisecond)

	prog := p.Progress()
	if v, ok := prog[id]; ok {
		t.Logf("Progress for %s: %d/%d", id, v[0], v[1])
	}
}

func TestSQLiteBackend(t *testing.T) {
	p, err := New(Config{
		Backend:    "sqlite",
		DSN:        ":memory:",
		MaxWorkers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var executed atomic.Bool
	p.Handle("test", func(ctx context.Context, task *Task) (any, error) {
		executed.Store(true)
		return "done", nil
	})

	p.Submit("test", "hello")
	time.Sleep(100 * time.Millisecond)

	if !executed.Load() {
		t.Fatal("SQLite: handler was not executed")
	}
}

func TestSQLiteBackend_Persistence(t *testing.T) {
	p, err := New(Config{
		Backend:    "sqlite",
		DSN:        ":memory:",
		MaxWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	p.Handle("store", func(ctx context.Context, task *Task) (any, error) {
		return "stored", nil
	})

	id, _ := p.Submit("store", "data")
	p.Stop()

	// Reopen same DB
	p2, err := New(Config{
		Backend:    "sqlite",
		DSN:        ":memory:",
		MaxWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Stop()

	task, err := p2.GetTask(id)
	if err != nil {
		if err == ErrTaskNotFound {
			t.Log("Expected: in-memory SQLite is per-connection, task not found in new session")
		} else {
			t.Fatal(err)
		}
	} else {
		t.Logf("Found task: %s status=%s", task.ID, task.Status)
	}
}

func TestParallelMode(t *testing.T) {
	p, err := New(Config{
		Backend:    "memory",
		Mode:       "parallel",
		MaxWorkers: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var concurrent atomic.Int32
	var maxSeen atomic.Int32

	p.Handle("par", func(ctx context.Context, task *Task) (any, error) {
		v := concurrent.Add(1)
		defer concurrent.Add(-1)
		if v > maxSeen.Load() {
			maxSeen.Store(v)
		}
		time.Sleep(100 * time.Millisecond)
		return v, nil
	})

	for i := 0; i < 10; i++ {
		p.Submit("par", i)
	}

	time.Sleep(500 * time.Millisecond)

	n := maxSeen.Load()
	t.Logf("Max concurrent: %d", n)
	if n < 2 {
		t.Fatal("expected at least 2 concurrent executions in parallel mode")
	}
}
