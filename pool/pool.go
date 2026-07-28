// Package pool 提供通用任务池，支持可插拔的存储后端。
//
// 支持的后端：memory（内存）、sqlite（SQLite）、mysql（MySQL）。
// 执行模式：parallel（并发）或 sequential（串行），由 MaxWorkers 控制。
//
// 快速开始：
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
//	// 使用选项提交任务
//	p.Submit("print", "hello", pool.WithPriority(1))
//	p.Submit("print", "delayed", pool.WithDelay(3*time.Second))
//	p.Submit("print", "batched", pool.WithBatchID("batch_001"))
//
//	// 注册回调
//	p.OnComplete(func(t *pool.Task) { log.Println("done:", t.ID) })
//	p.OnFailed(func(t *pool.Task, err error) { log.Println("failed:", err) })
//	p.OnProgress(func(id string, cur, total int) { fmt.Printf("%s: %d/%d\n", id, cur, total) })
//	p.OnBatchComplete(func(bID string, results []*pool.TaskResult) {
//	    fmt.Printf("batch %s complete: %d tasks\n", bID, len(results))
//	})
//
//	// 统计和进度
//	stats, _ := p.Stats()
//	progress := p.Progress()
package pool

// Version 是 pool 包的当前版本号。
const Version = "0.2.0"