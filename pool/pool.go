// Package pool provides a general-purpose task pool with pluggable storage backends.
//
// Supported backends: memory, sqlite, mysql.
// Execution modes: parallel or sequential (controlled by MaxWorkers).
//
// Quick start:
//
//	p, _ := pool.New(pool.Config{
//	    Backend: "memory",
//	    Mode:    "parallel",
//	    MaxWorkers: 5,
//	})
//	defer p.Stop()
//
//	p.Handle("print", func(ctx context.Context, t *pool.Task) (any, error) {
//	    fmt.Println(string(t.Data))
//	    return nil, nil
//	})
//
//	// Submit with options
//	p.Submit("print", "hello", pool.WithPriority(1))
//	p.Submit("print", "delayed", pool.WithDelay(3*time.Second))
//	p.Submit("print", "batched", pool.WithBatchID("batch_001"))
//
//	// Callbacks
//	p.OnComplete(func(t *pool.Task) { log.Println("done:", t.ID) })
//	p.OnFailed(func(t *pool.Task, err error) { log.Println("failed:", err) })
//	p.OnProgress(func(id string, cur, total int) { fmt.Printf("%s: %d/%d\n", id, cur, total) })
//	p.OnBatchComplete(func(bID string, results []*pool.TaskResult) {
//	    fmt.Printf("batch %s complete: %d tasks\n", bID, len(results))
//	})
//
//	// Stats and progress
//	stats, _ := p.Stats()
//	progress := p.Progress()
package pool

// Version of the pool package.
const Version = "0.2.0"
