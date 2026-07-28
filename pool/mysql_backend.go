package pool

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// mysqlBackend 将任务存储在 MySQL 数据库中。
type mysqlBackend struct {
	db     *sql.DB
	cfg    Config
	notify chan struct{} // 唤醒阻塞的 Dequeue
}

func newMySQLBackend(cfg Config) (Backend, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("%w: MySQL DSN 是必需的", ErrInvalidConfig)
	}
	return &mysqlBackend{
		cfg:    cfg,
		notify: make(chan struct{}, 1),
	}, nil
}

func init() {
	registerBackend("mysql", newMySQLBackend)
}

func (b *mysqlBackend) Init(ctx context.Context) error {
	db, err := sql.Open("mysql", b.cfg.DSN)
	if err != nil {
		return fmt.Errorf("pool/mysql: 打开数据库失败: %w", err)
	}
	b.db = db

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pool/mysql: 连接测试失败: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS taskpool_tasks (
			id            VARCHAR(64) PRIMARY KEY,
			type          VARCHAR(128) NOT NULL,
			data          LONGTEXT,
			status        TINYINT NOT NULL DEFAULT 0,
			priority      INT NOT NULL DEFAULT 0,
			batch_id      VARCHAR(64) DEFAULT '',
			timeout_ms    INT NOT NULL DEFAULT 0,
			max_retries   INT NOT NULL DEFAULT 0,
			retries       INT NOT NULL DEFAULT 0,
			scheduled_at  DATETIME(3) NULL,
			created_at    DATETIME(3) NOT NULL,
			started_at    DATETIME(3) NULL,
			done_at       DATETIME(3) NULL,
			error         TEXT,
			result        LONGTEXT,
			progress_current INT NOT NULL DEFAULT 0,
			progress_total   INT NOT NULL DEFAULT 0,
			metadata      LONGTEXT,
			INDEX idx_status (status),
			INDEX idx_batch (batch_id),
			INDEX idx_scheduled (scheduled_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`)
	if err != nil {
		return fmt.Errorf("pool/mysql: 创建表失败: %w", err)
	}
	return nil
}

func (b *mysqlBackend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *mysqlBackend) Save(task *Task) error {
	if task.ID == "" {
		task.ID = newID()
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}

	_, err := b.db.Exec(
		`INSERT INTO taskpool_tasks (id, type, data, status, priority, batch_id, timeout_ms, max_retries, retries, scheduled_at, created_at, error, progress_current, progress_total, metadata)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		task.ID, task.Type, string(task.Data), task.Status, task.Priority, task.BatchID,
		int(task.Timeout.Milliseconds()), task.MaxRetries, task.Retries,
		nullTime(task.ScheduledAt), task.CreatedAt, task.Error,
		task.ProgressCurrent, task.ProgressTotal, string(task.Metadata),
	)
	return err
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

const mysqlSelectCols = `id, type, COALESCE(data,''), status, priority, COALESCE(batch_id,''), timeout_ms, max_retries, retries, scheduled_at, created_at, started_at, done_at, COALESCE(error,''), COALESCE(result,''), progress_current, progress_total, metadata`

func (b *mysqlBackend) Get(id string) (*Task, error) {
	row := b.db.QueryRow(`SELECT `+mysqlSelectCols+` FROM taskpool_tasks WHERE id = ?`, id)
	return b.scanTask(row)
}

func (b *mysqlBackend) Delete(id string) error {
	_, err := b.db.Exec(`DELETE FROM taskpool_tasks WHERE id = ?`, id)
	return err
}

func (b *mysqlBackend) Enqueue(task *Task) error {
	existing, err := b.Get(task.ID)
	if err != nil && err != ErrTaskNotFound {
		return err
	}
	if existing != nil {
		_, err = b.db.Exec(
			`UPDATE taskpool_tasks SET status=?, priority=?, scheduled_at=? WHERE id=?`,
			task.Status, task.Priority, nullTime(task.ScheduledAt), task.ID,
		)
		if err == nil {
			b.wakeUp()
		}
		return err
	}
	if err := b.Save(task); err != nil {
		return err
	}
	b.wakeUp()
	return nil
}

func (b *mysqlBackend) Dequeue(ctx context.Context) (*Task, error) {
	return b.dequeue(ctx, 0)
}

func (b *mysqlBackend) DequeueTimeout(ctx context.Context, timeout time.Duration) (*Task, error) {
	return b.dequeue(ctx, timeout)
}

func (b *mysqlBackend) dequeue(ctx context.Context, timeout time.Duration) (*Task, error) {
	pollInterval := b.cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	for {
		task, err := b.claimNextTask()
		if err != nil {
			return nil, err
		}
		if task != nil {
			return task, nil
		}

		select {
		case <-b.notify:
			continue
		case <-timeoutCh:
			return nil, context.DeadlineExceeded
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
			continue
		}
	}
}

// claimNextTask 在事务中选取并锁定下一个可执行任务。
// 使用 SELECT ... FOR UPDATE SKIP LOCKED 避免竞争（需要 MySQL 8.0+）。
func (b *mysqlBackend) claimNextTask() (*Task, error) {
	now := time.Now()

	tx, err := b.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// SELECT ... FOR UPDATE SKIP LOCKED（MySQL 8.0+）
	row := tx.QueryRow(`
		SELECT `+mysqlSelectCols+` FROM taskpool_tasks
		WHERE status = 0 AND (scheduled_at IS NULL OR scheduled_at <= ?)
		ORDER BY priority ASC, created_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, now)

	task, err := b.scanTask(row)
	if err != nil {
		if err == ErrTaskNotFound {
			return nil, nil
		}
		return nil, err
	}

	_, err = tx.Exec(
		`UPDATE taskpool_tasks SET status = ?, started_at = ? WHERE id = ? AND status = 0`,
		StatusRunning, now, task.ID,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	task.Status = StatusRunning
	task.StartedAt = &now
	return task, nil
}

func (b *mysqlBackend) Pending() int {
	var count int
	b.db.QueryRow(`SELECT COUNT(*) FROM taskpool_tasks WHERE status = 0`).Scan(&count)
	return count
}

func (b *mysqlBackend) Remove(id string) bool {
	result, err := b.db.Exec(
		`UPDATE taskpool_tasks SET status = ? WHERE id = ? AND status IN (0, 1)`,
		StatusCancelled, id,
	)
	if err != nil {
		return false
	}
	affected, _ := result.RowsAffected()
	return affected > 0
}

func (b *mysqlBackend) UpdateStatus(id string, status TaskStatus, errStr string) error {
	now := time.Now()
	var doneAt interface{}
	if status == StatusCompleted || status == StatusFailed || status == StatusCancelled {
		doneAt = now
	}
	_, err := b.db.Exec(
		`UPDATE taskpool_tasks SET status=?, error=?, done_at=? WHERE id=?`,
		status, errStr, doneAt, id,
	)
	return err
}

func (b *mysqlBackend) UpdateProgress(id string, current, total int) error {
	_, err := b.db.Exec(
		`UPDATE taskpool_tasks SET progress_current=?, progress_total=? WHERE id=?`,
		current, total, id,
	)
	return err
}

func (b *mysqlBackend) UpdateResult(id string, result []byte) error {
	_, err := b.db.Exec(`UPDATE taskpool_tasks SET result=? WHERE id=?`, string(result), id)
	return err
}

func (b *mysqlBackend) ListByBatchID(batchID string) ([]*Task, error) {
	rows, err := b.db.Query(
		`SELECT `+mysqlSelectCols+` FROM taskpool_tasks WHERE batch_id = ? ORDER BY created_at ASC`,
		batchID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return b.scanTasks(rows)
}

func (b *mysqlBackend) ListByStatus(status TaskStatus, limit, offset int) ([]*Task, error) {
	rows, err := b.db.Query(
		`SELECT `+mysqlSelectCols+` FROM taskpool_tasks WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		status, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return b.scanTasks(rows)
}

func (b *mysqlBackend) ListAll(limit, offset int) ([]*Task, error) {
	rows, err := b.db.Query(
		`SELECT `+mysqlSelectCols+` FROM taskpool_tasks ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return b.scanTasks(rows)
}

func (b *mysqlBackend) CountByStatus() (map[TaskStatus]int, error) {
	counts := map[TaskStatus]int{
		StatusPending: 0, StatusDelayed: 0, StatusRunning: 0,
		StatusCompleted: 0, StatusFailed: 0, StatusCancelled: 0, StatusRetrying: 0,
	}
	rows, err := b.db.Query(`SELECT status, COUNT(*) FROM taskpool_tasks GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var status int
		var count int
		if rows.Scan(&status, &count) == nil {
			counts[TaskStatus(status)] = count
		}
	}
	return counts, nil
}

func (b *mysqlBackend) CancelBatch(batchID string) (int, error) {
	result, err := b.db.Exec(
		`UPDATE taskpool_tasks SET status = ? WHERE batch_id = ? AND status IN (0, 1)`,
		StatusCancelled, batchID,
	)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return int(affected), nil
}

func (b *mysqlBackend) Recover(ctx context.Context) error {
	_, err := b.db.Exec(
		`UPDATE taskpool_tasks SET status = 0, started_at = NULL, error = '' WHERE status = 2`,
	)
	return err
}

func (b *mysqlBackend) UpdateMetadata(id string, metadata json.RawMessage) error {
	_, err := b.db.Exec(`UPDATE taskpool_tasks SET metadata=? WHERE id=?`, string(metadata), id)
	return err
}
func (b *mysqlBackend) wakeUp() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

// ------- 扫描辅助函数 -------

type mysqlScanner interface {
	Scan(dest ...interface{}) error
}

func (b *mysqlBackend) scanTask(s mysqlScanner) (*Task, error) {
	t := &Task{}
	var (
		data, result, errStr string
		status, priority, timeoutMs, maxRetries, retries int
		progressCurr, progressTotal int
		scheduled, created, started, done sql.NullTime
		meta sql.NullString
	)
	err := s.Scan(
		&t.ID, &t.Type, &data, &status, &priority, &t.BatchID,
		&timeoutMs, &maxRetries, &retries,
		&scheduled, &created, &started, &done,
		&errStr, &result, &progressCurr, &progressTotal,
		&meta,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}
	t.Status = TaskStatus(status)
	t.Priority = priority
	t.MaxRetries = maxRetries
	t.Retries = retries
	t.Timeout = time.Duration(timeoutMs) * time.Millisecond
	t.Error = errStr
	t.ProgressCurrent = progressCurr
	t.ProgressTotal = progressTotal

	if data != "" {
		t.Data = json.RawMessage(data)
	}
	if result != "" {
		t.Result = json.RawMessage(result)
	}
	if scheduled.Valid {
		t.ScheduledAt = scheduled.Time
	}
	t.CreatedAt = created.Time
	if started.Valid {
		t.StartedAt = &started.Time
	}
	if done.Valid {
		t.DoneAt = &done.Time
	}
	return t, nil
}

func (b *mysqlBackend) scanTasks(rows *sql.Rows) ([]*Task, error) {
	var tasks []*Task
	for rows.Next() {
		t, err := b.scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}


