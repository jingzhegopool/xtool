package pool

import (
	"context"
	"testing"
	"time"
)

// TestExample demonstrates the complete task pool usage flow.
func TestExample(t *testing.T) {
	p := New(Config{
		MinWorkers: 2,
		MaxWorkers: 10,
	})
	defer p.Stop()

	p.Handle("greet", func(ctx context.Context, job *Job) (any, error) {
		var name string
		if err := job.Decode(&name); err != nil {
			return nil, err
		}
		return "Hello, " + name + "!", nil
	})

	id, err := p.Submit("greet", "World", WithPriority(1))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	time.Sleep(100 * time.Millisecond)

	stats := p.Stats()
	t.Logf("Task ID: %s", id)
	t.Logf("Queued: %d, Done: %d, Workers: %d",
		stats.TasksQueued, stats.TasksDone, stats.WorkersTotal)

	if stats.TasksDone != 1 {
		t.Errorf("expected 1 completed task, got %d", stats.TasksDone)
	}
}
