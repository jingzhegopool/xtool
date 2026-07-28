# xtool

> Go 零依赖工具集合库 — 由 jingzhegopool 维护

## 子包

| 子包 | import | 说明 | 状态 |
|------|--------|------|------|
| `pool` | `github.com/jingzhegopool/xtool/pool` | goroutine 任务池，带优先级队列 | ✅ v0.1.0 |
| `slice` | *(coming)* | 切片操作工具 | 📋 规划 |
| `retry` | *(coming)* | 重试机制 | 📋 规划 |
| `concurrent` | *(coming)* | 并发辅助 | 📋 规划 |

## 快速上手

```go
import "github.com/jingzhegopool/xtool/pool"

func main() {
    p := pool.New(pool.Config{MinWorkers: 5, MaxWorkers: 50})
    defer p.Stop()

    p.Handle("email", func(ctx context.Context, job *pool.Job) (any, error) {
        var to string
        job.Decode(&to)
        return "sent", nil
    })

    id, _ := p.Submit("email", "user@example.com", pool.WithPriority(1))
    _ = id
}
```

## 原则

- **零外部依赖** — 只使用 Go 标准库
- **开箱即用** — 所有子包均有合理默认值
- **纯净** — 不引入 `vendor`，不做过度抽象

## 许可

MIT
