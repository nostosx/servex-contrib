//go:build integration

package asynq

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
)

// Run with: SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 go test -tags integration ./...
// Every test uses its own asynq queue, so a shared server can be used.

func redisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("SERVEX_IT_REDIS_ADDR")
	if addr == "" {
		t.Skip("SERVEX_IT_REDIS_ADDR is not set")
	}
	return addr
}

func TestIntegrationQueue(t *testing.T) {
	addr := redisAddr(t)
	runSuite(t, backend{
		client: func(t *testing.T) redis.UniversalClient {
			return redis.NewClient(&redis.Options{Addr: addr})
		},
		advance: func(_ *testing.T, d time.Duration) { time.Sleep(d) },
		ttl:     time.Second,
		cleanup: cleanupQueue,
	})
}

func TestIntegrationNew(t *testing.T) {
	addr := redisAddr(t)
	q, err := New(asynq.RedisClientOpt{Addr: addr}, WithQueue("it-new-"+uuid.NewV7().String()),
		WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	insp := asynq.NewInspectorFromRedisClient(q.rdb)
	t.Cleanup(func() { cleanupQueue(t, q) })
	id, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.WithDelay(time.Hour), jobqueue.WithMaxRetry(7))
	if err != nil {
		t.Fatal(err)
	}
	info, err := insp.GetTaskInfo(q.opts.queue, id)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != asynq.TaskStateScheduled || info.MaxRetry != 7 || time.Until(info.NextProcessAt) < 59*time.Minute {
		t.Fatalf("task info = %+v", info)
	}
	if err := insp.DeleteQueue(q.opts.queue, true); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{ID: id, Type: "work"}); err != nil {
		t.Fatalf("Enqueue() after the queue was deleted = %v", err)
	}
	_, err = q.Enqueue(t.Context(), &jobqueue.Job{ID: id, Type: "work"})
	if errorx.CodeOf(err) != errorx.AlreadyExists {
		t.Fatalf("Enqueue(same ID) = %v, want AlreadyExists", err)
	}
}

// cleanupQueue only touches the UUID queue and its own uniqueness reservations.
func cleanupQueue(t *testing.T, q *Queue) {
	t.Helper()
	if err := q.inspector.DeleteQueue(q.opts.queue, true); err != nil && !errors.Is(err, asynq.ErrQueueNotFound) {
		t.Errorf("cleanup queue %q: %v", q.opts.queue, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	keys, err := q.rdb.Keys(ctx, q.uniqueKey("")+"*").Result()
	if err != nil {
		t.Errorf("find queue reservations: %v", err)
		return
	}
	if len(keys) > 0 {
		if err := q.rdb.Del(ctx, keys...).Err(); err != nil {
			t.Errorf("delete queue reservations: %v", err)
		}
	}
}
