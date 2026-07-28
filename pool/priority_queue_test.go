package pool

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestPriorityQueue_EnqueueDequeue(t *testing.T) {
	q := newPriorityQueue(100)

	jobs := []*Job{
		{ID: "a", Priority: 3, CreatedAt: time.Now()},
		{ID: "b", Priority: 1, CreatedAt: time.Now().Add(1 * time.Second)},
		{ID: "c", Priority: 2, CreatedAt: time.Now().Add(2 * time.Second)},
	}
	for _, j := range jobs {
		if err := q.Enqueue(j); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	got1, _ := q.Dequeue(context.Background())
	if got1.ID != "b" {
		t.Errorf("expected b, got %s", got1.ID)
	}
	got2, _ := q.Dequeue(context.Background())
	if got2.ID != "c" {
		t.Errorf("expected c, got %s", got2.ID)
	}
	got3, _ := q.Dequeue(context.Background())
	if got3.ID != "a" {
		t.Errorf("expected a, got %s", got3.ID)
	}
}

func TestPriorityQueue_FIFOForSamePriority(t *testing.T) {
	q := newPriorityQueue(100)

	now := time.Now()
	jobs := []*Job{
		{ID: "first", Priority: 1, CreatedAt: now},
		{ID: "second", Priority: 1, CreatedAt: now.Add(1 * time.Millisecond)},
		{ID: "third", Priority: 1, CreatedAt: now.Add(2 * time.Millisecond)},
	}
	for _, j := range jobs {
		q.Enqueue(j)
	}

	for _, expected := range []string{"first", "second", "third"} {
		job, err := q.Dequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if job.ID != expected {
			t.Errorf("expected %s, got %s", expected, job.ID)
		}
	}
}

func TestPriorityQueue_BlockingDequeue(t *testing.T) {
	q := newPriorityQueue(100)

	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Enqueue(&Job{ID: "late", Priority: 1, CreatedAt: time.Now()})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	job, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue blocked for too long: %v", err)
	}
	if job.ID != "late" {
		t.Errorf("expected late, got %s", job.ID)
	}
}

func TestPriorityQueue_ContextCancel(t *testing.T) {
	q := newPriorityQueue(100)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := q.Dequeue(ctx)
	if err == nil {
		t.Fatal("expected context deadline exceeded error")
	}
}

func TestPriorityQueue_QueueFull(t *testing.T) {
	q := newPriorityQueue(2)

	q.Enqueue(&Job{ID: "a", Priority: 1})
	q.Enqueue(&Job{ID: "b", Priority: 1})

	err := q.Enqueue(&Job{ID: "c", Priority: 1})
	if err != ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestPriorityQueue_CloseWakeup(t *testing.T) {
	q := newPriorityQueue(100)

	errCh := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(context.Background())
		errCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case err := <-errCh:
		if err != ErrQueueClosed {
			t.Fatalf("expected ErrQueueClosed, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue didn't wake up after Close")
	}
}

func TestPriorityQueue_Remove(t *testing.T) {
	q := newPriorityQueue(100)

	q.Enqueue(&Job{ID: "keep", Priority: 1})
	q.Enqueue(&Job{ID: "remove-me", Priority: 1})
	q.Enqueue(&Job{ID: "keep2", Priority: 1})

	if !q.Remove("remove-me") {
		t.Fatal("Remove returned false")
	}
	if q.Remove("nonexistent") {
		t.Fatal("Remove should return false for nonexistent ID")
	}

	ids := make(map[string]bool)
	for i := 0; i < 2; i++ {
		job, _ := q.Dequeue(context.Background())
		ids[job.ID] = true
	}
	if !ids["keep"] || !ids["keep2"] {
		t.Errorf("unexpected results: %v", ids)
	}
	if ids["remove-me"] {
		t.Error("remove-me was still in queue")
	}
}

func TestPriorityQueue_LargeScale(t *testing.T) {
	q := newPriorityQueue(10000)
	n := 5000

	for i := 0; i < n; i++ {
		q.Enqueue(&Job{
			ID:        fmt.Sprintf("job-%d", i),
			Priority:  rand.Intn(100),
			CreatedAt: time.Now(),
		})
	}

	prevPriority := -1
	for i := 0; i < n; i++ {
		job, err := q.Dequeue(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if job.Priority < prevPriority {
			t.Fatalf("priority decreased: %d < %d at index %d", job.Priority, prevPriority, i)
		}
		prevPriority = job.Priority
	}
}

func TestPriorityQueue_PendingCount(t *testing.T) {
	q := newPriorityQueue(100)
	if q.Pending() != 0 {
		t.Fatalf("expected 0 pending, got %d", q.Pending())
	}

	q.Enqueue(&Job{ID: "a"})
	if q.Pending() != 1 {
		t.Fatalf("expected 1 pending, got %d", q.Pending())
	}

	q.Enqueue(&Job{ID: "b"})
	if q.Pending() != 2 {
		t.Fatalf("expected 2 pending, got %d", q.Pending())
	}

	q.Dequeue(context.Background())
	if q.Pending() != 1 {
		t.Fatalf("expected 1 pending after dequeue, got %d", q.Pending())
	}
}
