// Package pgmq adapts PGMQ >= 1.5 to servex workers. The caller installs the
// extension and owns the database pool. Delivery is at least once; handlers must
// make external side effects idempotent and honor cancellation.
package pgmq

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/nostosx/servex-contrib/database/postgres"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
)

// ErrLeaseLost means another delivery owns the receipt, or its visibility expired.
var ErrLeaseLost = errors.New("pgmq: delivery lease lost")

type Config struct {
	Queue                           string
	Reservations                    postgres.Table
	Concurrency                     int
	VisibilityTimeout, PollInterval time.Duration
}
type Queue struct {
	db                *sql.DB
	cfg               Config
	table, queueTable string
	owner             context.Context
	cancel            context.CancelFunc
	mu                sync.Mutex
	closed            bool
	active            int
	done              chan struct{}
}

var _ jobqueue.Enqueuer = (*Queue)(nil)
var _ jobqueue.ReadyRunner = (*Queue)(nil)

func New(db *sql.DB, cfg Config) (*Queue, error) {
	if db == nil {
		return nil, postgres.Invalid("nil database")
	}
	if _, err := postgres.Ident(cfg.Queue); err != nil {
		return nil, err
	}
	if len(cfg.Queue) > 40 {
		return nil, postgres.Invalid("queue name exceeds 40 bytes")
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.VisibilityTimeout == 0 {
		cfg.VisibilityTimeout = 30 * time.Second
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.Concurrency < 1 || cfg.Concurrency > 128 || cfg.VisibilityTimeout < 3*time.Second || cfg.VisibilityTimeout > 24*time.Hour || cfg.VisibilityTimeout%time.Second != 0 || cfg.PollInterval < 10*time.Millisecond || cfg.PollInterval > time.Minute {
		return nil, postgres.Invalid("invalid concurrency, visibility timeout or poll interval")
	}
	if cfg.Reservations.Schema == "" {
		cfg.Reservations.Schema = "public"
	}
	if cfg.Reservations.Name == "" {
		cfg.Reservations.Name = "servex_pgmq_" + cfg.Queue
	}
	t, err := cfg.Reservations.SQL()
	if err != nil {
		return nil, err
	}
	qt, err := postgres.Ident("pgmq", "q_"+cfg.Queue)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{db: db, cfg: cfg, table: t, queueTable: qt, owner: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

func (q *Queue) operation(ctx context.Context) (context.Context, func(), error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil, nil, errorx.New(errorx.FailedPrecondition, "PGMQ_CLOSED", "pgmq: queue closed")
	}
	q.active++
	q.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(q.owner, cancel)
	return ctx, func() {
		stop()
		cancel()
		q.mu.Lock()
		q.active--
		if q.closed && q.active == 0 {
			close(q.done)
		}
		q.mu.Unlock()
	}, nil
}

// Close cancels active calls and waits within ctx. It never closes the pool.
// A later Close can continue waiting for a handler that ignored cancellation.
func (q *Queue) Close(ctx context.Context) error {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		q.cancel()
		if q.active == 0 {
			close(q.done)
		}
	}
	q.mu.Unlock()
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Migrate creates the queue and this adapter's uniqueness table atomically.
func (q *Queue) Migrate(ctx context.Context) error {
	ctx, finish, err := q.operation(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if err := postgres.Require(ctx, q.db, "pgmq"); err != nil {
		return err
	}
	return postgres.Transaction(ctx, q.db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `SELECT pgmq.create($1)`, q.cfg.Queue); err != nil {
			return postgres.Error(err)
		}
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+q.table+` (key text COLLATE "C" PRIMARY KEY,owner text NOT NULL,expires_at timestamptz)`)
		return postgres.Error(err)
	})
}

type envelope struct {
	Job            jobqueue.Job `json:"job"`
	MaxRetry       int          `json:"max_retry"`
	UniqueKey      string       `json:"unique_key,omitempty"`
	LifetimeUnique bool         `json:"lifetime_unique,omitempty"`
	failureReason  string
}
type prepared struct {
	id     string
	body   []byte
	opts   jobqueue.EnqueueOptions
	unique string
}

func validateJob(job *jobqueue.Job) error {
	if job == nil || strings.TrimSpace(job.Type) == "" || len(job.Type) > 256 || len(job.Payload) > 1024*1024 {
		return postgres.Invalid("invalid job type or payload")
	}
	if len(job.ID) > 1024 || strings.ContainsRune(job.ID, 0) {
		return postgres.Invalid("invalid job ID")
	}
	return nil
}

func prepare(job *jobqueue.Job, opts []jobqueue.EnqueueOption) (prepared, error) {
	if err := validateJob(job); err != nil {
		return prepared{}, err
	}
	j := *job
	if j.ID == "" {
		j.ID = rand.Text()
	}
	j.Attempt = 0
	o := jobqueue.ApplyOptions(opts...)
	if o.MaxRetry > 1000 || o.Delay > 365*24*time.Hour || len(o.UniqueKey) > 1024 || strings.ContainsRune(o.UniqueKey, 0) || o.UniqueTTL > 365*24*time.Hour {
		return prepared{}, postgres.Invalid("invalid enqueue options")
	}
	e := envelope{Job: j, MaxRetry: o.MaxRetry, UniqueKey: o.UniqueKey, LifetimeUnique: o.UniqueTTL <= 0}
	body, err := json.Marshal(e)
	if err == nil && len(body) > 2*1024*1024 {
		return prepared{}, postgres.Invalid("encoded job exceeds 2 MiB")
	}
	return prepared{j.ID, body, o, o.UniqueKey}, err
}
func seconds(d time.Duration) int64 { return int64(math.Ceil(d.Seconds())) }

func (q *Queue) Enqueue(ctx context.Context, job *jobqueue.Job, opts ...jobqueue.EnqueueOption) (string, error) {
	p, err := prepare(job, opts)
	if err != nil {
		return "", err
	}
	ctx, finish, err := q.operation(ctx)
	if err != nil {
		return "", err
	}
	defer finish()
	err = postgres.Transaction(ctx, q.db, func(tx *sql.Tx) error { return q.enqueue(ctx, tx, p) })
	if err != nil {
		return "", err
	}
	return p.id, nil
}

// EnqueueTx joins a caller's transaction. The caller must roll back on any error,
// including ErrDuplicate; successful return does not mean its transaction committed.
func (q *Queue) EnqueueTx(ctx context.Context, tx *sql.Tx, job *jobqueue.Job, opts ...jobqueue.EnqueueOption) (string, error) {
	if tx == nil {
		return "", postgres.Invalid("nil transaction")
	}
	p, err := prepare(job, opts)
	if err != nil {
		return "", err
	}
	ctx, finish, err := q.operation(ctx)
	if err != nil {
		return "", err
	}
	defer finish()
	if err := q.enqueue(ctx, tx, p); err != nil {
		return "", err
	}
	return p.id, nil
}

// ScheduleCommand builds trusted SQL suitable for pgcron.Schedule. Each execution
// creates a fresh PostgreSQL UUID, reserves it and sends the job in one statement.
// Fixed IDs/UniqueKey are rejected: a recurring template must not accidentally
// reuse business idempotency keys. This function performs no database I/O.
func (q *Queue) ScheduleCommand(job *jobqueue.Job, opts ...jobqueue.EnqueueOption) (string, error) {
	if job == nil || job.ID != "" {
		return "", postgres.Invalid("scheduled templates must not have a fixed job ID")
	}
	p, err := prepare(job, opts)
	if err != nil {
		return "", err
	}
	if p.unique != "" {
		return "", postgres.Invalid("scheduled templates do not support unique keys")
	}
	delimiter := "$servex_" + rand.Text() + "$"
	for strings.Contains(string(p.body), delimiter) {
		delimiter = "$servex_" + rand.Text() + "$"
	}
	// Queue is a validated identifier; dollar quoting protects the opaque JSON.
	return `WITH job AS MATERIALIZED (SELECT gen_random_uuid()::text AS id), reservation AS (` +
		`INSERT INTO ` + q.table + ` (key,owner) SELECT 'id:'||id,id FROM job RETURNING owner) ` +
		`SELECT pgmq.send('` + q.cfg.Queue + `'::text,jsonb_set(` + delimiter + string(p.body) + delimiter +
		`::jsonb,'{job,ID}',to_jsonb(owner)),` + fmt.Sprint(seconds(p.opts.Delay)) + `::integer) FROM reservation`, nil
}
func (q *Queue) reserve(ctx context.Context, tx *sql.Tx, key, owner string, ttl time.Duration) error {
	var expiry any
	if ttl > 0 {
		expiry = ttl.Seconds()
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO `+q.table+` (key,owner,expires_at) VALUES($1,$2,CASE WHEN $3::double precision IS NULL THEN NULL ELSE clock_timestamp()+$3*interval '1 second' END) ON CONFLICT(key) DO UPDATE SET owner=EXCLUDED.owner,expires_at=EXCLUDED.expires_at WHERE `+q.table+`.expires_at<=clock_timestamp()`, key, owner, expiry)
	if err != nil {
		return postgres.Error(err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return jobqueue.ErrDuplicate
	}
	return nil
}
func (q *Queue) enqueue(ctx context.Context, tx *sql.Tx, p prepared) error {
	if err := q.reserve(ctx, tx, "id:"+p.id, p.id, 0); err != nil {
		return err
	}
	if p.unique != "" {
		if err := q.reserve(ctx, tx, "unique:"+p.unique, p.id, p.opts.UniqueTTL); err != nil {
			return err
		}
	}
	var id int64
	return postgres.Error(tx.QueryRowContext(ctx, `SELECT pgmq.send($1,$2::jsonb,$3::integer)`, q.cfg.Queue, string(p.body), seconds(p.opts.Delay)).Scan(&id))
}

type delivery struct {
	id, receipt int64
	body        []byte
}

func (q *Queue) read(ctx context.Context) (delivery, error) {
	var d delivery
	err := q.db.QueryRowContext(ctx, `SELECT msg_id,read_ct,message FROM pgmq.read($1,$2::integer,1)`, q.cfg.Queue, seconds(q.cfg.VisibilityTimeout)).Scan(&d.id, &d.receipt, &d.body)
	return d, err
}
func (q *Queue) Run(ctx context.Context, handlers map[string]endpoint.Endpoint) error {
	return q.RunReady(ctx, handlers, func() {})
}
func (q *Queue) RunReady(ctx context.Context, handlers map[string]endpoint.Endpoint, ready func()) error {
	if ready == nil || len(handlers) == 0 {
		return postgres.Invalid("handlers and readiness callback are required")
	}
	copyHandlers := make(map[string]endpoint.Endpoint, len(handlers))
	for key, h := range handlers {
		if h == nil {
			return postgres.Invalid("nil handler")
		}
		copyHandlers[key] = h
	}
	ctx, finish, err := q.operation(ctx)
	if err != nil {
		return err
	}
	defer finish()
	// Verify both resources before reporting ready; migrations are never implicit.
	for _, table := range []string{q.queueTable, q.table} {
		rows, err := q.db.QueryContext(ctx, `SELECT 1 FROM `+table+` LIMIT 0`)
		if err != nil {
			return postgres.Error(err)
		}
		if err := rows.Close(); err != nil {
			return postgres.Error(err)
		}
	}
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, q.cfg.Concurrency)
	var wg sync.WaitGroup
	for range q.cfg.Concurrency {
		wg.Go(func() {
			if err := q.work(run, copyHandlers); err != nil && !errors.Is(err, context.Canceled) {
				failures <- err
				cancel()
			}
		})
	}
	ready()
	wg.Wait()
	select {
	case err := <-failures:
		return err
	default:
		return nil
	}
}
func (q *Queue) work(ctx context.Context, handlers map[string]endpoint.Endpoint) error {
	ticker := time.NewTicker(q.cfg.PollInterval)
	defer ticker.Stop()
	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		d, err := q.read(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			failures = 0
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				continue
			}
		}
		if err != nil {
			err = postgres.Error(err)
		} else {
			err = q.process(ctx, d, handlers)
		}
		if err == nil || errors.Is(err, ErrLeaseLost) {
			failures = 0
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		if !errorx.IsRetryable(err) {
			return err
		}
		failures++
		delay := min(30*time.Second, max(q.cfg.PollInterval, 100*time.Millisecond)*time.Duration(1<<min(failures-1, 8)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}
func call(ctx context.Context, h endpoint.Endpoint, job *jobqueue.Job) (err error) {
	defer func() {
		if recover() != nil {
			err = errorx.New(errorx.Internal, "PGMQ_HANDLER_PANIC", "pgmq: handler panicked")
		}
	}()
	_, err = h(ctx, job)
	return err
}
func (q *Queue) process(ctx context.Context, d delivery, handlers map[string]endpoint.Endpoint) error {
	var e envelope
	decodeErr := json.Unmarshal(d.body, &e)
	if decodeErr != nil || len(d.body) > 2*1024*1024 || validateJob(&e.Job) != nil || e.MaxRetry < 0 || e.MaxRetry > 1000 || e.Job.ID == "" || len(e.UniqueKey) > 1024 || strings.ContainsRune(e.UniqueKey, 0) {
		e.failureReason = "invalid-envelope"
		return q.settle(ctx, d, e, false, true, 0)
	}
	if d.receipt > int64(e.MaxRetry)+1 {
		e.failureReason = "attempts-exhausted"
		return q.settle(ctx, d, e, false, true, 0)
	}
	job := e.Job
	job.Attempt = int(d.receipt)
	h := handlers[e.Job.Type]
	if h == nil {
		e.failureReason = "missing-handler"
		return q.settle(ctx, d, e, false, true, 0)
	}
	run, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatDone := make(chan error, 1)
	go func() { heartbeatDone <- q.heartbeat(run, d, cancel) }()
	handlerErr := call(run, h, &job)
	cancel()
	heartbeatErr := <-heartbeatDone
	if heartbeatErr != nil {
		return heartbeatErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	retry := handlerErr != nil && errorx.IsRetryable(handlerErr) && d.receipt <= int64(e.MaxRetry)
	if handlerErr != nil {
		e.failureReason = fmt.Sprint(errorx.CodeOf(handlerErr))
	}
	delay := time.Second * time.Duration(int64(1)<<min(d.receipt-1, 6))
	return q.settle(ctx, d, e, retry, handlerErr != nil && !retry, delay)
}
func (q *Queue) heartbeat(ctx context.Context, d delivery, cancel context.CancelFunc) error {
	ticker := time.NewTicker(q.cfg.VisibilityTimeout / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			// Lock first, then check server time: waiting must not revive an expired lease.
			result, err := q.db.ExecContext(ctx, `WITH locked AS MATERIALIZED (SELECT msg_id,read_ct,vt FROM `+q.queueTable+` WHERE msg_id=$1 FOR UPDATE) UPDATE `+q.queueTable+` AS message SET vt=clock_timestamp()+$3*interval '1 second' FROM locked WHERE message.msg_id=locked.msg_id AND locked.read_ct=$2 AND locked.vt>clock_timestamp()`, d.id, d.receipt, seconds(q.cfg.VisibilityTimeout))
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				cancel()
				return postgres.Error(err)
			}
			n, err := result.RowsAffected()
			if err != nil {
				cancel()
				return err
			}
			if n != 1 {
				cancel()
				return ErrLeaseLost
			}
		}
	}
}
func (q *Queue) settle(ctx context.Context, d delivery, e envelope, retry, archive bool, delay time.Duration) error {
	return postgres.Transaction(ctx, q.db, func(tx *sql.Tx) error {
		var receipt int64
		var live bool
		err := tx.QueryRowContext(ctx, `WITH locked AS MATERIALIZED (SELECT read_ct,vt FROM `+q.queueTable+` WHERE msg_id=$1 FOR UPDATE) SELECT read_ct,vt>clock_timestamp() FROM locked`, d.id).Scan(&receipt, &live)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return postgres.Error(err)
		}
		if receipt != d.receipt || !live {
			return ErrLeaseLost
		}
		if retry {
			var id int64
			return postgres.Error(tx.QueryRowContext(ctx, `SELECT msg_id FROM pgmq.set_vt($1::text,$2::bigint,$3::integer)`, q.cfg.Queue, d.id, seconds(delay)).Scan(&id))
		}
		function := "delete"
		if archive {
			// Store classification, not a potentially sensitive handler error message.
			_, err := tx.ExecContext(ctx, `UPDATE `+q.queueTable+` SET message=CASE WHEN jsonb_typeof(message)='object' THEN message ELSE jsonb_build_object('_servex_original',message) END || jsonb_build_object('_servex_failure',jsonb_build_object('attempt',$2::bigint,'at',clock_timestamp(),'reason',$3::text)) WHERE msg_id=$1`, d.id, d.receipt, e.failureReason)
			if err != nil {
				return postgres.Error(err)
			}
			function = "archive"
		}
		var settled bool
		if err := tx.QueryRowContext(ctx, `SELECT pgmq.`+function+`($1::text,$2::bigint)`, q.cfg.Queue, d.id).Scan(&settled); err != nil {
			return postgres.Error(err)
		}
		if !settled {
			return ErrLeaseLost
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM `+q.table+` WHERE owner=$1 AND (key=$2 OR (key=$3 AND expires_at IS NULL))`, e.Job.ID, "id:"+e.Job.ID, "unique:"+e.UniqueKey)
		return postgres.Error(err)
	})
}

// PruneExpired removes a bounded batch of expired positive-TTL reservations.
// Lifetime reservations are retained until terminal settlement. If operators
// delete queue messages outside this adapter, they must reconcile those reservations.
func (q *Queue) PruneExpired(ctx context.Context, limit int) (int64, error) {
	if limit < 1 || limit > 10000 {
		return 0, postgres.Invalid("prune limit must be 1..10000")
	}
	ctx, finish, err := q.operation(ctx)
	if err != nil {
		return 0, err
	}
	defer finish()
	r, err := q.db.ExecContext(ctx, `WITH expired AS (SELECT key FROM `+q.table+` WHERE expires_at<=clock_timestamp() LIMIT $1 FOR UPDATE SKIP LOCKED) DELETE FROM `+q.table+` WHERE key IN (SELECT key FROM expired)`, limit)
	if err != nil {
		return 0, postgres.Error(err)
	}
	return r.RowsAffected()
}

func (q *Queue) String() string { return fmt.Sprintf("pgmq:%s", q.cfg.Queue) }

// Length counts queued and in-flight messages, excluding the archive. It is a
// monitoring snapshot, not a guarantee that all external side effects completed.
func (q *Queue) Length(ctx context.Context) (int64, error) {
	ctx, finish, err := q.operation(ctx)
	if err != nil {
		return 0, err
	}
	defer finish()
	var count int64
	err = q.db.QueryRowContext(ctx, `SELECT count(*) FROM `+q.queueTable).Scan(&count)
	return count, postgres.Error(err)
}
