package pool

import (
	"container/heap"
	"context"
	"sync"
)

// priorityQueue is a thread-safe priority queue based on a binary heap.
// Lower priority numbers are dequeued first; FIFO ordering for equal priorities.
type priorityQueue struct {
	mu     sync.Mutex
	items  heapItems
	cap    int
	closed bool
	wait   chan struct{} // closed on enqueue/close to wake waiting dequeuers
}

func newPriorityQueue(cap int) *priorityQueue {
	return &priorityQueue{
		items: make(heapItems, 0, 64),
		cap:   cap,
		wait:  make(chan struct{}),
	}
}

// Enqueue adds a job to the queue. Returns an error if the queue is full or closed.
func (pq *priorityQueue) Enqueue(job *Job) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return ErrQueueClosed
	}
	if len(pq.items) >= pq.cap {
		return ErrQueueFull
	}

	heap.Push(&pq.items, job)

	// Wake up all goroutines blocked on Dequeue
	close(pq.wait)
	pq.wait = make(chan struct{})
	return nil
}

// Dequeue blocks until a job is available or the context is cancelled.
func (pq *priorityQueue) Dequeue(ctx context.Context) (*Job, error) {
	pq.mu.Lock()

	for {
		if len(pq.items) > 0 {
			job := heap.Pop(&pq.items).(*Job)
			pq.mu.Unlock()
			return job, nil
		}
		if pq.closed {
			pq.mu.Unlock()
			return nil, ErrQueueClosed
		}

		wait := pq.wait
		pq.mu.Unlock()

		select {
		case <-wait:
			// New item arrived or queue closed, re-check
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		pq.mu.Lock()
	}
}

// Pending returns the number of jobs waiting in the queue.
func (pq *priorityQueue) Pending() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.items)
}

// Remove removes a job by ID from the queue. Returns true if found and removed.
func (pq *priorityQueue) Remove(id string) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for i, job := range pq.items {
		if job.ID == id {
			heap.Remove(&pq.items, i)
			return true
		}
	}
	return false
}

// Close closes the queue and wakes up all blocked Dequeue callers.
func (pq *priorityQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if !pq.closed {
		pq.closed = true
		close(pq.wait)
	}
}

// --- container/heap interface ---

// heapItems implements heap.Interface, ordered by (Priority, CreatedAt).
type heapItems []*Job

func (h heapItems) Len() int            { return len(h) }
func (h heapItems) Less(i, j int) bool {
	if h[i].Priority != h[j].Priority {
		return h[i].Priority < h[j].Priority // lower number first
	}
	return h[i].CreatedAt.Before(h[j].CreatedAt) // FIFO
}
func (h heapItems) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *heapItems) Push(x any)        { *h = append(*h, x.(*Job)) }
func (h *heapItems) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
