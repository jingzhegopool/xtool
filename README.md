# xtool — Go Toolkit by jingzhegopool

> 零外部核心依赖的工具集合库。`pool` 子包提供通用任务池，支持内存/SQLite/MySQL 三种后端。

## 子包

| 子包 | import | 说明 | 状态 |
|------|--------|------|------|
| `pool` | `github.com/jingzhegopool/xtool/pool` | 通用任务池 | ✅ v0.2.0 |
| `slice` | *(coming)* | 切片操作工具 | 📋 规划 |
| `retry` | *(coming)* | 重试机制 | 📋 规划 |

## pool 使用示例

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/jingzhegopool/xtool/pool"
)

func main() {
    // 内存后端，5 个并发 worker
    p, err := pool.New(pool.Config{
        Backend:    "memory",
        Mode:       "parallel",
        MaxWorkers: 5,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer p.Stop()

    // 注册任务处理器
    p.Handle("greet", func(ctx context.Context, t *pool.Task) (any, error) {
        var name string
        t.Decode(&name)
        return "Hello, " + name, nil
    })

    // 提交任务
    p.Submit("greet", "World", pool.WithPriority(1))
    p.Submit("greet", "Delayed", pool.WithDelay(3*time.Second))
    p.Submit("greet", "Batch", pool.WithBatchID("batch_001"))

    // 进度回调
    p.OnProgress(func(id string, cur, total int) {
        log.Printf("task %s: %d/%d", id, cur, total)
    })

    // 批完成回调
    p.OnBatchComplete(func(batchID string, results []*pool.TaskResult) {
        log.Printf("batch %s complete: %d tasks", batchID, len(results))
    })

    time.Sleep(5 * time.Second)

    stats, _ := p.Stats()
    log.Printf("completed: %d, failed: %d",
        stats[pool.StatusCompleted], stats[pool.StatusFailed])
}
```

## 配置

### 选择后端

```go
// 内存（零依赖）
p, _ := pool.New(pool.Config{Backend: "memory"})

// SQLite（自动创建表）
p, _ := pool.New(pool.Config{
    Backend: "sqlite",
    DSN:     "./tasks.db",
})

// MySQL（自动创建表）
p, _ := pool.New(pool.Config{
    Backend: "mysql",
    DSN:     "user:pass@tcp(127.0.0.1:3306)/dbname",
})
```

### 全部配置项

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `Backend` | `"memory"` | 存储后端: `memory`, `sqlite`, `mysql` |
| `DSN` | `""` | 数据库连接串 |
| `Mode` | `"parallel"` | 执行模式: `parallel`, `sequential` |
| `MaxWorkers` | `5` | 最大并发 worker 数 |
| `OnError` | `"continue"` | 失败策略: `continue`, `stop` |
| `MaxRetries` | `0` | 最大重试次数 |
| `DefaultTimeout` | `0` | 任务默认超时（0=无超时） |
| `PollInterval` | `200ms` | 数据库后端轮询间隔 |
| `MaxQueueSize` | `100000` | 内存后端队列容量 |
| `BatchCompleteCallback` | `true` | 是否启用批次完成回调 |

## 提交选项

```go
p.Submit("type", data)
p.Submit("type", data, pool.WithPriority(1))
p.Submit("type", data, pool.WithTimeout(10*time.Second))
p.Submit("type", data, pool.WithRetries(3))
p.Submit("type", data, pool.WithBatchID("batch_001"))
p.Submit("type", data, pool.WithDelay(5*time.Minute))
p.Submit("type", data, pool.WithScheduleAt(time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)))
```

## 特性

- ✅ 三种后端：内存 / SQLite(自动建表) / MySQL(自动建表)
- ✅ 并发 / 串行模式可选
- ✅ 任务优先级队列
- ✅ 任务超时控制
- ✅ 自动重试
- ✅ Python 恢复保护
- ✅ 延迟执行
- ✅ 批次支持 + 整批完成回调
- ✅ 进度跟踪
- ✅ 统计（各状态数量）
- ✅ 失败策略（继续 / 停止）
- ✅ 数据库表自动初始化

## 测试

```bash
cd pool
go test -v -race -count=1 ./...
```

## 许可

MIT
