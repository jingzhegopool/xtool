package pool

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteCols = "id, type, data, status, priority, batch_id, timeout_ms, max_retries, retries, scheduled_at, created_at, started_at, done_at, error, result, progress_current, progress_total"

// sqliteBackend 将任务存储在 SQLite 数据库中。
type sqliteBackend struct {
	db     *sql.DB
	cfg    Config
	dsn    string
	notify chan struct{} // 唤醒阻塞的 Dequeue
}

func newSQLiteBackend(cfg Config) (Backend, error) {
	dsn := cfg.DSN
	if dsn == "" {
		dsn = "./taskpool.db"
	}
	return &sqliteBackend{
		cfg:    cfg,
		dsn:    dsn,
		notify: make(chan struct{}, 1),
	}, nil
}

func init() {
	registerBackend("sqlite", newSQLiteBackend)
}

func (b *sqliteBackend) Init(ctx context.Context) error {
	db, err := sql.Open("sqlite", b.dsn)
	if err != nil {
		return fmt.Errorf("pool/sqlite: 打开数据库失败: %w", err)
	}
	b.db = db

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS taskpool_tasks (
			id            TEXT PRIMARY KEY,
			type          TEXT NOT NULL,
			data          TEXT,
			status        INTEGER NOT NULL DEFAULT 0,
			priority      INTEGER NOT NULL DEFAULT 0,
			batch_id      TEXT DEFAULT '',
			timeout_ms    INTEGER NOT NULL DEFAULT 0,
			max_retries   INTEGER NOT NULL DEFAULT 0,
			retries       INTEGER NOT NULL DEFAULT 0,
			scheduled_at  TEXT,
			created_at    TEXT NOT NULL,
			started_at    TEXT,
			done_at       TEXT,
			error         TEXT DEFAULT '',
			result        TEXT,
			progress_current INTEGER NOT NULL DEFAULT 0,
			progress_total   INTEGER NOT NULL DEFAULT 0
		)
	`)
	if err != nil {
		return fmt.Errorf("pool/sqlite: 创建表失败: %w", err)
	}

	db.Exec(`CREATE INDEX IF NOT EXISTS idx_tp_status ON taskpool_tasks(status)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_tp_batch ON taskpool_tasks(batch_id)`)
	return nil
}

func (b *sqliteBackend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *sqliteBackend) Save(task *Task) error {
	if task.ID == "" {
		task.ID = newID()
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	_, err := b.db.Exec(
		`INSERT INTO taskpool_tasks (`+sqliteCols+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		task.ID, task.Type, string(task.Data), task.Status, task.Priority, task.BatchID,
		int(task.Timeout.Milliseconds()), task.MaxRetries, task.Retries,
		sqltime(task.ScheduledAt), sqltime(task.CreatedAt),
		sqltime(zeroTime(task.StartedAt)), sqltime(zeroTime(task.DoneAt)),
		task.Error, string(task.Result), task.ProgressCurrent, task.ProgressTotal,
	)
	return err
}

func zeroTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func sqltime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.Format(time.RFC3339Nano)
}

func (b *sqliteBackend) Get(id string) (*Task, error) {
	row := b.db.QueryRow(`SELECT `+sqliteCols+` FROM taskpool_tasks WHERE id = ?`, id)
	return b.scanTask(row)
}

func (b *sqliteBackend) Delete(id string) error {
	_, err := b.db.Exec(`DELETE FROM taskpool_tasks WHERE id = ?`, id)
	return err
}

func (b *sqliteBackend) Enqueue(task *Task) error {
	existing, err := b.Get(task.ID)
	if err != nil && err != ErrTaskNotFound {
		return err
	}
	if existing == nil {
		if err := b.Save(task); err != nil {
			return err
		}
	} else {
		_, err = b.db.Exec(
			`UPDATE taskpool_tasks SET status=?, priority=?, scheduled_at=? WHERE id=?`,
			task.Status, task.Priority, sqltime(task.ScheduledAt), task.ID,
		)
		if err != nil {
			return err
		}
	}
	b.wakeUp()
	return nil
}

func (b *sqliteBackend) Dequeue(ctx context.Context) (*Task, error) {
	return b.dequeue(ctx, 0)
}

func (b *sqliteBackend) DequeueTimeout(ctx context.Context, timeout time.Duration) (*Task, error) {
	return b.dequeue(ctx, timeout)
}

func (b *sqliteBackend) dequeue(ctx context.Context, timeout time.Duration) (*Task, error) {
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

// claimNextTask 选取优先级最高的待处理任务，并将其状态标记为"运行中"。
func (b *sqliteBackend) claimNextTask() (*Task, error) {
	now := time.Now().Format(time.RFC3339Nano)

	row := b.db.QueryRow(`
		SELECT id FROM taskpool_tasks
		WHERE status = 0 AND (scheduled_at IS NULL OR scheduled_at = '' OR scheduled_at <= ?)
		ORDER BY priority ASC, created_at ASC
		LIMIT 1
	`, now)

	var id string
	if err := row.Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	res, err := b.db.Exec(
		`UPDATE taskpool_tasks SET status = ?, started_at = ? WHERE id = ? AND status = 0`,
		StatusRunning, now, id,
	)
	if err != nil {
		return nil, err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return nil, nil
	}
	return b.Get(id)
}

func (b *sqliteBackend) Pending() int {
	var n int
	b.db.QueryRow(`SELECT COUNT(*) FROM taskpool_tasks WHERE status = 0`).Scan(&n)
	return n
}

func (b *sqliteBackend) Remove(id string) bool {
	res, err := b.db.Exec(
		`UPDATE taskpool_tasks SET status = ? WHERE id = ? AND status IN (0, 1)`,
		StatusCancelled, id,
	)
	if err != nil {
		return false
	}
	n, _ := res.RowsAffected()
	return n > 0
}

func (b *sqliteBackend) UpdateStatus(id string, status TaskStatus, errStr string) error {
	now := time.Now().Format(time.RFC3339Nano)
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

func (b *sqliteBackend) UpdateProgress(id string, current, total int) error {
	_, err := b.db.Exec(
		`UPDATE taskpool_tasks SET progress_current=?, progress_total=? WHERE id=?`,
		current, total, id,
	)
	return err
}

func (b *sqliteBackend) UpdateResult(id string, result []byte) error {
	_, err := b.db.Exec(`UPDATE taskpool_tasks SET result=? WHERE id=?`, string(result), id)
	return err
}

func (b *sqliteBackend) ListByBatchID(batchID string) ([]*Task, error) {
	rows, err := b.db.Query(`SELECT `+sqliteCols+` FROM taskpool_tasks WHERE batch_id = ? ORDER BY created_at ASC`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return b.scanTasks(rows)
}

func (b *sqliteBackend) ListByStatus(status TaskStatus, limit, offset int) ([]*Task, error) {
	rows, err := b.db.Query(`SELECT `+sqliteCols+` FROM taskpool_tasks WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`, status, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return b.scanTasks(rows)
}

func (b *sqliteBackend) ListAll(limit, offset int) ([]*Task, error) {
	rows, err := b.db.Query(`SELECT `+sqliteCols+` FROM taskpool_tasks ORDER BY created_at DESC LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return b.scanTasks(rows)
}

func (b *sqliteBackend) scanTasks(rows *sql.Rows) ([]*Task, error) {
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

type sqliteScanner interface {
	Scan(dest ...interface{}) error
}

func (b *sqliteBackend) scanTask(s sqliteScanner) (*Task, error) {
	t := &Task{}
	var (
		data, result, errStr                             sql.NullString
		status, priority, timeoutMs, maxRetries, retries int
		progressCurr, progressTotal                      int
		scheduled, created, started, done                sql.NullString
	)
	err := s.Scan(
		&t.ID, &t.Type, &data, &status, &priority, &t.BatchID,
		&timeoutMs, &maxRetries, &retries,
		&scheduled, &created, &started, &done,
		&errStr, &result, &progressCurr, &progressTotal,
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
	t.ProgressCurrent = progressCurr
	t.ProgressTotal = progressTotal

	if data.Valid {
		t.Data = json.RawMessage(data.String)
	}
	if result.Valid {
		t.Result = json.RawMessage(result.String)
	}
	t.Error = errStr.String
	t.CreatedAt = parseTime(created.String)
	t.ScheduledAt = parseTime(scheduled.String)
	if started.Valid {
		pt := parseTime(started.String)
		t.StartedAt = &pt
	}
	if done.Valid {
		pt := parseTime(done.String)
		t.DoneAt = &pt
	}
	return t, nil
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func (b *sqliteBackend) CountByStatus() (map[TaskStatus]int, error) {
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

func (b *sqliteBackend) CancelBatch(batchID string) (int, error) {
	res, err := b.db.Exec(
		`UPDATE taskpool_tasks SET status = ? WHERE batch_id = ? AND status IN (0, 1)`,
		StatusCancelled, batchID,
	)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// wakeUp 唤醒阻塞中的 Dequeue。
func (b *sqliteBackend) wakeUp() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}
