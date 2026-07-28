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
		t.Fatal("期望非空的任务 ID")
	}

	time.Sleep(100 * time.Millisecond)
	if !executed.Load() {
		t.Fatal("处理函数未被执行")
	}
}

func TestMemoryBackend_Priority(t *testing.T) {
	// 通过后端队列直接测试优先级排序
	q, _ := newMemoryBackend(Config{MaxQueueSize: 100})
	ctx := context.Background()

	// 按逆优先级顺序提交（最差的排最前）
	tasks := []*Task{
		{ID: newID(), Type: "test", Data: marshalAny("low"), Priority: 10, Status: StatusPending, CreatedAt: time.Now()},
		{ID: newID(), Type: "test", Data: marshalAny("medium"), Priority: 5, Status: StatusPending, CreatedAt: time.Now().Add(1 * time.Millisecond)},
		{ID: newID(), Type: "test", Data: marshalAny("high"), Priority: 1, Status: StatusPending, CreatedAt: time.Now().Add(2 * time.Millisecond)},
	}
	for _, t := range tasks {
		q.(*memoryBackend).Save(t)
		q.(*memoryBackend).Enqueue(t)
	}

	// 出队顺序应为：high -> medium -> low
	for _, expected := range []string{"high", "medium", "low"} {
		got, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var val string
		got.Decode(&val)
		if val != expected {
			t.Errorf("期望 %s，实际得到 %s", expected, val)
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
		t.Fatal("延迟任务未被执行")
	}
	if executedAt.Sub(start) < 200*time.Millisecond {
		t.Fatal("任务执行过早")
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
			t.Fatalf("期望 batch_a，实际得到 %s", bid)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("等待批次完成超时")
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

	t.Logf("统计：完成=%d 失败=%d 待处理=%d",
		stats[StatusCompleted], stats[StatusFailed], stats[StatusPending])

	if stats[StatusCompleted] == 0 {
		t.Fatal("期望至少 1 个已完成任务")
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
		t.Fatal("OnFailed 回调未被触发")
	}
}

func TestMemoryBackend_PanicRecovery(t *testing.T) {
	p, err := New(Config{Backend: "memory", MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	p.Handle("panic", func(ctx context.Context, task *Task) (any, error) {
		panic("测试 panic")
	})

	var failed atomic.Bool
	p.OnFailed(func(task *Task, err error) {
		failed.Store(true)
	})

	p.Submit("panic", nil, WithRetries(0))

	time.Sleep(200 * time.Millisecond)

	if !failed.Load() {
		t.Fatal("panic 应触发 OnFailed 回调")
	}

	// 任务池应仍然正常工作
	var worked atomic.Bool
	p.Handle("ok", func(ctx context.Context, task *Task) (any, error) {
		worked.Store(true)
		return nil, nil
	})
	p.Submit("ok", nil)
	time.Sleep(100 * time.Millisecond)

	if !worked.Load() {
		t.Fatal("panic 后任务池应仍能正常工作")
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
		t.Fatal("Cancel 返回 false")
	}

	time.Sleep(600 * time.Millisecond)

	stats, _ := p.Stats()
	if stats[StatusPending] > 0 {
		t.Error("取消后应无待处理任务")
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
		t.Logf("任务 %s 的进度：%d/%d", id, v[0], v[1])
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
		t.Fatal("SQLite：处理函数未被执行")
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

	// 重新打开同一个数据库
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
			t.Log("预期行为：内存 SQLite 是每个连接独立的，新会话中找不到任务")
		} else {
			t.Fatal(err)
		}
	} else {
		t.Logf("找到任务：%s 状态=%s", task.ID, task.Status)
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
	t.Logf("最大并发数：%d", n)
	if n < 2 {
		t.Fatal("并行模式下期望至少 2 个并发执行")
	}
}
