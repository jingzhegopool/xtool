package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_SubmitAndWait(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 2})
	defer p.Stop()

	var executed atomic.Bool
	p.Handle("test", func(ctx context.Context, job *Job) (any, error) {
		executed.Store(true)
		return "done", nil
	})

	id, err := p.Submit("test", "hello")
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	if id == "" {
		t.Fatal("expected non-empty ID")
	}

	time.Sleep(100 * time.Millisecond)

	if !executed.Load() {
		t.Fatal("handler was not executed")
	}
}

func TestPool_MultipleHandlers(t *testing.T) {
	p := New(Config{MinWorkers: 2, MaxWorkers: 4})
	defer p.Stop()

	var sumA, sumB atomic.Int64

	p.Handle("add", func(ctx context.Context, job *Job) (any, error) {
		var nums []int64
		if err := job.Decode(&nums); err != nil {
			return nil, err
		}
		var s int64
		for _, n := range nums {
			s += n
		}
		sumA.Store(s)
		return s, nil
	})

	p.Handle("mul", func(ctx context.Context, job *Job) (any, error) {
		var nums []int64
		if err := job.Decode(&nums); err != nil {
			return nil, err
		}
		s := int64(1)
		for _, n := range nums {
			s *= n
		}
		sumB.Store(s)
		return s, nil
	})

	p.Submit("add", []int64{1, 2, 3, 4, 5})
	p.Submit("mul", []int64{2, 3, 4})

	time.Sleep(200 * time.Millisecond)

	if sumA.Load() != 15 {
		t.Errorf("expected sum=15, got %d", sumA.Load())
	}
	if sumB.Load() != 24 {
		t.Errorf("expected mul=24, got %d", sumB.Load())
	}
}

func TestPool_PauseResume(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 2})
	defer p.Stop()

	var count atomic.Int32
	p.Handle("work", func(ctx context.Context, job *Job) (any, error) {
		count.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil, nil
	})

	for i := 0; i < 10; i++ {
		p.Submit("work", i)
	}

	time.Sleep(100 * time.Millisecond)
	before := count.Load()

	p.Pause()
	time.Sleep(100 * time.Millisecond)
	afterPause := count.Load()

	if afterPause != before {
		t.Errorf("tasks executed during pause: before=%d after=%d", before, afterPause)
	}

	p.Resume()
	time.Sleep(200 * time.Millisecond)

	afterResume := count.Load()
	if afterResume == afterPause {
		t.Fatal("no new tasks executed after resume")
	}
}

func TestPool_CancelPending(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 1})
	defer p.Stop()

	p.Handle("slow", func(ctx context.Context, job *Job) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return nil, nil
	})

	p.Submit("slow", nil)
	time.Sleep(20 * time.Millisecond)

	id2, _ := p.Submit("slow", nil)

	if !p.Cancel(id2) {
		t.Fatal("Cancel returned false")
	}

	time.Sleep(300 * time.Millisecond)
	stats := p.Stats()
	if stats.TasksQueued != 0 {
		t.Errorf("expected 0 queued after cancel, got %d", stats.TasksQueued)
	}
}

func TestPool_Retry(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 2, DefaultRetries: 2})
	defer p.Stop()

	var attempts atomic.Int32
	p.Handle("flaky", func(ctx context.Context, job *Job) (any, error) {
		n := attempts.Add(1)
		if n < 3 {
			return nil, context.DeadlineExceeded
		}
		return "ok", nil
	})

	var completed atomic.Bool
	p.OnComplete(func(job *Job) {
		completed.Store(true)
	})

	p.Submit("flaky", nil,
		WithRetries(2),
		WithTimeout(100*time.Millisecond))

	time.Sleep(500 * time.Millisecond)

	if !completed.Load() {
		t.Fatal("task was not completed after retries")
	}
	if n := attempts.Load(); n != 3 {
		t.Errorf("expected 3 attempts, got %d", n)
	}
}

func TestPool_Stats(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 2})
	defer p.Stop()

	p.Handle("fast", func(ctx context.Context, job *Job) (any, error) {
		return nil, nil
	})

	for i := 0; i < 5; i++ {
		p.Submit("fast", i)
	}

	time.Sleep(200 * time.Millisecond)

	stats := p.Stats()
	if stats.TasksDone == 0 {
		t.Errorf("expected some completed tasks, got %d", stats.TasksDone)
	}
}

func TestPool_OnFailedCallback(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 2})
	defer p.Stop()

	p.Handle("fail", func(ctx context.Context, job *Job) (any, error) {
		return nil, context.DeadlineExceeded
	})

	var failed atomic.Bool
	p.OnFailed(func(job *Job, err error) {
		failed.Store(true)
	})

	p.Submit("fail", nil, WithRetries(0))

	time.Sleep(200 * time.Millisecond)

	if !failed.Load() {
		t.Fatal("OnFailed callback not triggered")
	}
}

func TestPool_PanicRecovery(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 2})
	defer p.Stop()

	p.Handle("panic", func(ctx context.Context, job *Job) (any, error) {
		panic("something went wrong")
	})

	var failed atomic.Bool
	p.OnFailed(func(job *Job, err error) {
		failed.Store(true)
	})

	p.Submit("panic", nil, WithRetries(0))

	time.Sleep(200 * time.Millisecond)

	if !failed.Load() {
		t.Fatal("panic handler should trigger OnFailed")
	}

	var worked atomic.Bool
	p.Handle("ok", func(ctx context.Context, job *Job) (any, error) {
		worked.Store(true)
		return nil, nil
	})
	p.Submit("ok", nil)
	time.Sleep(100 * time.Millisecond)

	if !worked.Load() {
		t.Fatal("pool should still work after panic recovery")
	}
}

func TestPool_PriorityOrder(t *testing.T) {
	p := New(Config{MinWorkers: 1, MaxWorkers: 1})
	defer p.Stop()

	var (
		orderMu sync.Mutex
		order   []string
	)
	p.Handle("task", func(ctx context.Context, job *Job) (any, error) {
		var val string
		if err := job.Decode(&val); err != nil {
			return nil, err
		}
		orderMu.Lock()
		order = append(order, val)
		orderMu.Unlock()
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	})

	// Pause first to submit all tasks before any worker picks them up
	p.Pause()
	time.Sleep(50 * time.Millisecond) // let workers enter paused state
	p.Submit("task", "low", WithPriority(10))
	p.Submit("task", "medium", WithPriority(5))
	p.Submit("task", "high", WithPriority(1))
	p.Resume()

	time.Sleep(500 * time.Millisecond)

	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) < 3 {
		t.Fatalf("expected 3 results, got %d", len(order))
	}
	if order[0] != "high" || order[1] != "medium" || order[2] != "low" {
		t.Errorf("unexpected order: %v", order)
	}
}
