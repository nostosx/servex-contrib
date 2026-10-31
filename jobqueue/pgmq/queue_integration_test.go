//go:build integration

package pgmq

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
)

func integrationQueue(t *testing.T) *Queue {
	t.Helper()
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set SERVEX_IT_POSTGRES_DSN to an owned fixture")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(16)
	q, err := New(db, Config{Queue: "sx_" + strings.ToLower(rand.Text()), VisibilityTimeout: 3 * time.Second, PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := q.Close(ctx); err != nil {
			t.Error(err)
		}
		if _, err := db.ExecContext(ctx, `SELECT pgmq.drop_queue($1)`, q.cfg.Queue); err != nil {
			t.Error(err)
		}
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+q.table); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	if err := q.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return q
}
func TestIntegrationTransactionsAndUniqueness(t *testing.T) {
	q := integrationQueue(t)
	tx, err := q.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.EnqueueTx(t.Context(), tx, &jobqueue.Job{ID: "rollback", Type: "task"}, jobqueue.WithUnique("key", 0)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.read(t.Context()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rolled back job visible: %v", err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "first", Type: "task"}, jobqueue.WithUnique("key", 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "duplicate", Type: "task"}, jobqueue.WithUnique("key", 0)); !errors.Is(err, jobqueue.ErrDuplicate) {
		t.Fatal(err)
	}
	d, err := q.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var e envelope
	if err := json.Unmarshal(d.body, &e); err != nil {
		t.Fatal(err)
	}
	if err := q.settle(t.Context(), d, e, false, false, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "duplicate", Type: "task"}, jobqueue.WithUnique("key", 0)); err != nil {
		t.Fatalf("lifetime reservation not released: %v", err)
	}
}
func TestIntegrationReceiptsAndRetries(t *testing.T) {
	q := integrationQueue(t)
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: "retry", Type: "task"}, jobqueue.WithMaxRetry(1)); err != nil {
		t.Fatal(err)
	}
	first, err := q.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var attempts []int
	handlers := map[string]endpoint.Endpoint{"task": func(_ context.Context, request any) (any, error) {
		j := request.(*jobqueue.Job)
		attempts = append(attempts, j.Attempt)
		if j.Attempt == 1 {
			return nil, errorx.New(errorx.Unavailable, "RETRY", "retry")
		}
		return nil, nil
	}}
	if err := q.process(t.Context(), first, handlers); err != nil {
		t.Fatal(err)
	}
	if _, err := q.db.ExecContext(t.Context(), "UPDATE "+q.queueTable+" SET vt=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	second, err := q.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if second.receipt != 2 {
		t.Fatalf("receipt %d", second.receipt)
	}
	var e envelope
	if err := json.Unmarshal(first.body, &e); err != nil {
		t.Fatal(err)
	}
	if err := q.settle(t.Context(), first, e, false, false, 0); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack accepted: %v", err)
	}
	if err := q.process(t.Context(), second, handlers); err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0] != 1 || attempts[1] != 2 {
		t.Fatalf("attempts %v", attempts)
	}
	if _, err := q.read(t.Context()); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("completed job remains: %v", err)
	}
}

func TestIntegrationScheduledTemplate(t *testing.T) {
	q := integrationQueue(t)
	command, err := q.ScheduleCommand(&jobqueue.Job{Type: "task", Payload: []byte(`a'\b; SELECT pg_sleep(99)`)})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := q.db.ExecContext(t.Context(), command); err != nil {
			t.Fatal(err)
		}
	}
	first, err := q.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var a, b envelope
	if err := json.Unmarshal(first.body, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second.body, &b); err != nil {
		t.Fatal(err)
	}
	if a.Job.ID == b.Job.ID || len(a.Job.ID) != 36 || string(a.Job.Payload) != `a'\b; SELECT pg_sleep(99)` {
		t.Fatalf("invalid scheduled envelope: %s %s", a.Job.ID, b.Job.ID)
	}
}

func TestIntegrationFailureArchive(t *testing.T) {
	q := integrationQueue(t)
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "permanent"}, jobqueue.WithUnique("lifetime", 0)); err != nil {
		t.Fatal(err)
	}
	d, err := q.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := q.process(t.Context(), d, map[string]endpoint.Endpoint{"permanent": func(context.Context, any) (any, error) {
		return nil, errorx.New(errorx.InvalidArgument, "INVALID", "private application detail")
	}}); err != nil {
		t.Fatal(err)
	}
	var reason string
	if err := q.db.QueryRowContext(t.Context(), `SELECT message->'_servex_failure'->>'reason' FROM pgmq."a_`+q.cfg.Queue+`" WHERE msg_id=$1`, d.id).Scan(&reason); err != nil || reason == "" || strings.Contains(reason, "private") {
		t.Fatalf("archive reason %q: %v", reason, err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "permanent"}, jobqueue.WithUnique("lifetime", 0)); err != nil {
		t.Fatalf("failed job retains lifetime uniqueness: %v", err)
	}
}
func TestIntegrationWorkersAndHeartbeat(t *testing.T) {
	q := integrationQueue(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if _, err := q.Enqueue(ctx, &jobqueue.Job{ID: "work", Type: "task"}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var calls atomic.Int32
	handlers := map[string]endpoint.Endpoint{"task": func(ctx context.Context, _ any) (any, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	go func() { done <- q.RunReady(ctx, handlers, func() {}) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Hold beyond the original visibility timeout while the SQL heartbeat renews.
	timer := time.NewTimer(3500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := q.read(ctx); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("active delivery became visible: %v", err)
	}
	close(release)
	deadline := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	for {
		var count int
		if err := q.db.QueryRowContext(ctx, "SELECT count(*) FROM "+q.queueTable).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
		select {
		case <-deadline.C:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
	if calls.Load() != 1 {
		t.Fatalf("delivery count %d", calls.Load())
	}
}

// A lock holder need not update the tuple. Lease validity must be checked after
// acquiring the row lock, even when PostgreSQL has no updated tuple to recheck.
func TestIntegrationLeaseExpiresWhileWaitingForLock(t *testing.T) {
	for _, mode := range []string{"settle", "heartbeat"} {
		t.Run(mode, func(t *testing.T) {
			q := integrationQueue(t)
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
			defer cancel()
			if _, err := q.Enqueue(ctx, &jobqueue.Job{Type: "task"}); err != nil {
				t.Fatal(err)
			}
			d, err := q.read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var e envelope
			if err := json.Unmarshal(d.body, &e); err != nil {
				t.Fatal(err)
			}
			lock, err := q.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lock.Rollback() }()
			var id int64
			if err := lock.QueryRowContext(ctx, "SELECT msg_id FROM "+q.queueTable+" WHERE msg_id=$1 FOR UPDATE", d.id).Scan(&id); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			run, stop := context.WithCancel(ctx)
			defer stop()
			go func() {
				if mode == "settle" {
					done <- q.settle(run, d, e, false, false, 0)
				} else {
					done <- q.heartbeat(run, d, stop)
				}
			}()
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				var waiting bool
				if err := q.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND position($1 in query)>0 AND pid<>pg_backend_pid())`, q.queueTable).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
				}
			}
			for {
				var expired bool
				if err := q.db.QueryRowContext(ctx, "SELECT vt<=clock_timestamp() FROM "+q.queueTable+" WHERE msg_id=$1", d.id).Scan(&expired); err != nil {
					t.Fatal(err)
				}
				if expired {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
				}
			}
			if err := lock.Commit(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("expired %s accepted: %v", mode, err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var count int
			if err := q.db.QueryRowContext(ctx, "SELECT count(*) FROM "+q.queueTable+" WHERE msg_id=$1", d.id).Scan(&count); err != nil || count != 1 {
				t.Fatalf("expired delivery removed: %d: %v", count, err)
			}
		})
	}
}

func TestIntegrationOversizedEnvelopeIsArchived(t *testing.T) {
	q := integrationQueue(t)
	e := envelope{Job: jobqueue.Job{ID: "oversized", Type: "task", Payload: make([]byte, 1024*1024+1)}}
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := q.db.QueryRowContext(t.Context(), "SELECT pgmq.send($1::text,$2::jsonb)", q.cfg.Queue, string(body)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	d, err := q.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := q.process(t.Context(), d, map[string]endpoint.Endpoint{"task": func(context.Context, any) (any, error) {
		t.Fatal("oversized envelope reached handler")
		return nil, nil
	}}); err != nil {
		t.Fatal(err)
	}
	var reason string
	if err := q.db.QueryRowContext(t.Context(), `SELECT message->'_servex_failure'->>'reason' FROM pgmq."a_`+q.cfg.Queue+`" WHERE msg_id=$1`, id).Scan(&reason); err != nil || reason != "invalid-envelope" {
		t.Fatalf("%s: %v", reason, err)
	}
}
