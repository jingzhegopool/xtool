package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// ---------- v0.4.0 任务生命周期控制测试 ----------

// TestStartDelayed 启动：延迟执行的任务，用户改变主意立即执行。
func TestStartDelayed(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var executed atomic.Bool
	p.Handle("delayed", func(ctx context.Context, task *Task) (any, error) {
		executed.Store(true)
		return nil, nil
	})

	id, _ := p.Submit("delayed", "data", WithDelay(time.Hour))

	// 确认还在 delayed 状态
	task, _ := p.GetTask(id)
	if task.Status != StatusDelayed {
		t.Fatalf("期望 delayed，实际 %s", task.Status)
	}

	// 启动：立即执行
	if err := p.Start(id); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	if !executed.Load() {
		t.Fatal("Start 后任务未立即执行")
	}

	task, _ = p.GetTask(id)
	if task.Status != StatusCompleted {
		t.Fatalf("期望 completed，实际 %s", task.Status)
	}
}

// TestStopDrain 停止：正在执行的小任务继续完成，不再启动新的小任务（排空语义）。
func TestStopDrain(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var itemsDone atomic.Int32

	p.Handle("drain", func(ctx context.Context, task *Task) (any, error) {
		for i := 0; i < 100; i++ {
			// 小任务之间检查 ctx（排空语义的关键：当前小任务完成后再停）
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			time.Sleep(10 * time.Millisecond) // 模拟小任务执行
			itemsDone.Add(1)
		}
		return "all-done", nil
	})

	id, _ := p.Submit("drain", nil)

	// 等任务跑起来（至少完成几个小任务）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && itemsDone.Load() < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	if itemsDone.Load() < 3 {
		t.Fatal("任务未正常启动")
	}

	// 停止
	if err := p.StopTask(id); err != nil {
		t.Fatal(err)
	}

	// 等待排空完成（状态变 paused = handler 已返回）
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusPaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	task, _ := p.GetTask(id)
	if task.Status != StatusPaused {
		t.Fatalf("期望 paused，实际 %s", task.Status)
	}

	// 排空验证：handler 已退出，小任务数量不再增长
	after := itemsDone.Load()
	time.Sleep(150 * time.Millisecond)
	if n := itemsDone.Load(); n != after {
		t.Fatalf("停止后仍执行了 %d 个小任务（排空前 %d），不应启动新的小任务", n-after, after)
	}
}

// TestStopDrainFinishesCurrentItem 排空语义：正在执行的小任务不会被中途打断。
func TestStopDrainFinishesCurrentItem(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	// 小任务执行中不检查 ctx（模拟正在执行的小任务）
	itemRunning := make(chan struct{}, 1)
	itemDone := make(chan struct{}, 1)

	p.Handle("drain2", func(ctx context.Context, task *Task) (any, error) {
		for i := 0; i < 100; i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			itemRunning <- struct{}{}
			time.Sleep(30 * time.Millisecond) // 当前小任务执行中，不响应 cancel
			itemDone <- struct{}{}
		}
		return "all-done", nil
	})

	id, _ := p.Submit("drain2", nil)

	// 等第一个小任务开始执行
	select {
	case <-itemRunning:
	case <-time.After(2 * time.Second):
		t.Fatal("小任务未开始")
	}

	// 小任务执行中停止
	if err := p.StopTask(id); err != nil {
		t.Fatal(err)
	}

	// 当前小任务应执行完成（不被中途打断）
	select {
	case <-itemDone:
	case <-time.After(2 * time.Second):
		t.Fatal("正在执行的小任务被打断")
	}

	// 最终状态 paused，且不再执行新小任务
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusPaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	task, _ := p.GetTask(id)
	if task.Status != StatusPaused {
		t.Fatalf("期望 paused，实际 %s", task.Status)
	}
}

// TestStopNoRetry 停止不应触发重试：handler 响应 cancel 返回 context.Canceled。
func TestStopNoRetry(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1, MaxRetries: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("noretry", func(ctx context.Context, task *Task) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	id, _ := p.Submit("noretry", nil, WithRetries(3))
	time.Sleep(50 * time.Millisecond)

	if err := p.StopTask(id); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusPaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	task, _ := p.GetTask(id)
	if task.Status != StatusPaused {
		t.Fatalf("期望 paused（不重试），实际 %s", task.Status)
	}
}

// TestContinue 停止后继续执行：paused → pending → 重新执行。
func TestContinue(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var runs atomic.Int32
	p.Handle("cont", func(ctx context.Context, task *Task) (any, error) {
		if runs.Add(1) == 1 {
			// 第一次运行：阻塞直到被停止
			<-ctx.Done()
			return nil, ctx.Err()
		}
		// 第二次运行（Continue 后）：正常完成
		return "done", nil
	})

	id, _ := p.Submit("cont", nil)
	time.Sleep(50 * time.Millisecond)
	if err := p.StopTask(id); err != nil {
		t.Fatal(err)
	}

	// 等变 paused
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusPaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := p.Continue(id); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	task, _ := p.GetTask(id)
	if task.Status != StatusCompleted {
		t.Fatalf("Continue 后期望 completed，实际 %s", task.Status)
	}
	if runs.Load() != 2 {
		t.Fatalf("Continue 后应重新执行一次，运行次数=%d", runs.Load())
	}
}

// TestTerminateRunning 终止运行中任务：终态 cancelled，不重试。
func TestTerminateRunning(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("term", func(ctx context.Context, task *Task) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	id, _ := p.Submit("term", nil)
	time.Sleep(50 * time.Millisecond)

	if err := p.Terminate(id); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusCancelled {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	task, _ := p.GetTask(id)
	if task.Status != StatusCancelled {
		t.Fatalf("期望 cancelled，实际 %s", task.Status)
	}
}

// TestTerminatePending 终止排队中任务：直接 cancelled，不执行。
func TestTerminatePending(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var executed atomic.Bool
	p.Handle("tp", func(ctx context.Context, task *Task) (any, error) {
		executed.Store(true)
		return nil, nil
	})

	// 第一个任务占住 worker
	p.Handle("block", func(ctx context.Context, task *Task) (any, error) {
		time.Sleep(200 * time.Millisecond)
		return nil, nil
	})
	p.Submit("block", nil)
	time.Sleep(20 * time.Millisecond)

	// 第二个任务排队中
	id2, _ := p.Submit("tp", nil)
	time.Sleep(20 * time.Millisecond)

	if err := p.Terminate(id2); err != nil {
		t.Fatal(err)
	}

	time.Sleep(250 * time.Millisecond)
	if executed.Load() {
		t.Fatal("被终止的排队任务不应执行")
	}
	task, _ := p.GetTask(id2)
	if task.Status != StatusCancelled {
		t.Fatalf("期望 cancelled，实际 %s", task.Status)
	}
}

// TestStartFailed 失败后重新启动：重试计数清零，第二次成功。
func TestStartFailed(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	var runs atomic.Int32
	p.Handle("retryme", func(ctx context.Context, task *Task) (any, error) {
		if runs.Add(1) == 1 {
			return nil, context.DeadlineExceeded // 第一次必失败
		}
		return "ok", nil
	})

	id, _ := p.Submit("retryme", nil, WithRetries(0))

	time.Sleep(100 * time.Millisecond)
	task, _ := p.GetTask(id)
	if task.Status != StatusFailed {
		t.Fatalf("期望 failed，实际 %s", task.Status)
	}

	// 重新启动
	if err := p.Start(id); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	task, _ = p.GetTask(id)
	if task.Status != StatusCompleted {
		t.Fatalf("Start 后期望 completed，实际 %s", task.Status)
	}
	task, _ = p.GetTask(id)
	if task.Retries != 0 {
		t.Fatalf("Start 应清零重试计数，实际 retries=%d", task.Retries)
	}
}

// TestRemoveRunningRejected 移除限制：运行中任务不可移除。
func TestRemoveRunningRejected(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("long", func(ctx context.Context, task *Task) (any, error) {
		time.Sleep(500 * time.Millisecond)
		return nil, nil
	})

	id, _ := p.Submit("long", nil)
	time.Sleep(50 * time.Millisecond)

	// 运行中移除 → 拒绝
	if err := p.Remove(id); err != ErrTaskNotRemovable {
		t.Fatalf("运行中移除应返回 ErrTaskNotRemovable，实际 %v", err)
	}

	// 停止后再移除 → 成功
	if err := p.StopTask(id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusPaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := p.Remove(id); err != nil {
		t.Fatalf("停止后移除失败: %v", err)
	}
	if _, err := p.GetTask(id); err != ErrTaskNotFound {
		t.Fatalf("移除后应查不到任务，实际 %v", err)
	}
}

// TestRemoveAfterTerminate 终止后移除。
func TestRemoveAfterTerminate(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("tr", func(ctx context.Context, task *Task) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	id, _ := p.Submit("tr", nil)
	time.Sleep(50 * time.Millisecond)

	if err := p.Terminate(id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusCancelled {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := p.Remove(id); err != nil {
		t.Fatalf("终止后移除失败: %v", err)
	}
	if _, err := p.GetTask(id); err != ErrTaskNotFound {
		t.Fatalf("移除后应查不到任务，实际 %v", err)
	}
}

// TestConcurrentStop 并发安全：100 goroutine 同时停止同一任务。
func TestConcurrentStop(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("cstop", func(ctx context.Context, task *Task) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})

	id, _ := p.Submit("cstop", nil)
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 100; i++ {
		go func() {
			_ = p.StopTask(id) // 不 panic 即可
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, _ := p.GetTask(id)
		if task.Status == StatusPaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	task, _ := p.GetTask(id)
	if task.Status != StatusPaused {
		t.Fatalf("期望 paused，实际 %s", task.Status)
	}
}
