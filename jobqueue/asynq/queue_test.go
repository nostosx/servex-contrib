package asynq

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/endpoint"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/messaging/jobqueue"
	"github.com/nostosx/servex/metadata"
)

// miniBackend returns a backend on an in-process miniredis server.
func miniBackend(t *testing.T) (backend, *miniredis.Miniredis) {
	t.Helper()
	m := miniredis.RunT(t)
	return backend{
		client: func(t *testing.T) redis.UniversalClient {
			// No retries: a closed server is detected quickly.
			return redis.NewClient(&redis.Options{Addr: m.Addr(), MaxRetries: -1})
		},
		advance: func(_ *testing.T, d time.Duration) { m.FastForward(d) },
		ttl:     time.Hour,
	}, m
}

func TestQueueWithMiniredis(t *testing.T) {
	b, _ := miniBackend(t)
	runSuite(t, b)
}

func TestNew(t *testing.T) {
	m := miniredis.RunT(t)
	q, err := New(asynq.RedisClientOpt{Addr: m.Addr()}, WithLogger(testLogger(slog.New(slog.DiscardHandler))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}); err != nil {
		t.Fatalf("Enqueue() = %v", err)
	}
	q.Close()
	// The Queue owns the client created by New.
	if err := q.rdb.Ping(t.Context()).Err(); !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("Ping() after Close = %v, want ErrClosed", err)
	}
	if _, err := New(nil); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Fatalf("New(nil) = %v, want InvalidArgument", err)
	}
}

func TestNewFromClientDoesNotCloseClient(t *testing.T) {
	m := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer rdb.Close()
	q := NewFromClient(rdb)
	q.Close()
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("client unusable after Close: %v", err)
	}
}

func TestDefaults(t *testing.T) {
	q := NewFromClient(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), nil)
	defer q.rdb.Close()
	defer q.Close()
	o := q.opts
	if o.queue != "default" || o.concurrency != 4 || o.shutdownTimeout != 8*time.Second ||
		o.healthCheckInterval != 15*time.Second || o.backoff == nil || o.logger != nil {
		t.Fatalf("defaults = %+v", o)
	}
	if got := NewFromClient(q.rdb, WithConcurrency(0)).opts.concurrency; got != 1 {
		t.Fatalf("WithConcurrency(0) = %d, want 1", got)
	}
	for attempt, want := range map[int]time.Duration{
		0: 100 * time.Millisecond, 1: 100 * time.Millisecond, 2: 200 * time.Millisecond,
		4: 800 * time.Millisecond, 8: 10 * time.Second, 100: 10 * time.Second,
	} {
		if got := defaultBackoff(attempt); got != want {
			t.Errorf("defaultBackoff(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestQueueName(t *testing.T) {
	b, _ := miniBackend(t)
	for _, tt := range []struct {
		name string
		opts []Option
		want string
	}{
		{"empty", []Option{WithQueue("")}, defaultQueue},
		{"whitespace", []Option{WithQueue(" \t\n")}, defaultQueue},
		{"custom then blank", []Option{WithQueue("work"), WithQueue(" \t")}, "work"},
		{"padded name", []Option{WithQueue(" work ")}, " work "},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rdb := b.client(t)
			defer rdb.Close()
			q := NewFromClient(rdb, tt.opts...)
			defer q.Close()
			id, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"})
			if err != nil {
				t.Fatalf("Enqueue() = %v", err)
			}
			info, err := q.inspector.GetTaskInfo(tt.want, id)
			if err != nil {
				t.Fatalf("GetTaskInfo(%q, %q) = %v", tt.want, id, err)
			}
			if info.Queue != tt.want {
				t.Errorf("task queue = %q, want %q", info.Queue, tt.want)
			}
		})
	}
}

func TestHeaders(t *testing.T) {
	tests := []struct {
		name           string
		md             metadata.MD
		reservationKey string
	}{
		{"nil", nil, ""},
		{"single", metadata.Pairs("a", "1", "b", "2"), ""},
		{"multi", metadata.Join(metadata.Pairs("a", "1", "b", "2"), metadata.Pairs("a", "3")), ""},
		{"reservation key", metadata.Pairs("a", "1"), "servex:jobqueue:unique:q:k"},
		{"reservation key only", nil, "key"},
	}
	for _, tt := range tests {
		h, err := encodeHeaders(tt.md, tt.reservationKey)
		if err != nil {
			t.Fatalf("%s: encodeHeaders() = %v", tt.name, err)
		}
		md, reservationKey := decodeHeaders(h)
		if reservationKey != tt.reservationKey {
			t.Errorf("%s: reservation key = %q, want %q", tt.name, reservationKey, tt.reservationKey)
		}
		if len(md) != len(tt.md) {
			t.Errorf("%s: header = %v, want %v", tt.name, md, tt.md)
		}
		for k, vs := range tt.md.All() {
			if got := md.Values(k); !slices.Equal(got, vs) {
				t.Errorf("%s: header %s = %v, want %v", tt.name, k, got, vs)
			}
		}
	}
	// Plain asynq headers written by other producers are accepted.
	md, _ := decodeHeaders(map[string]string{"X-Request-ID": "r1"})
	if md.Get("x-request-id") != "r1" {
		t.Fatalf("plain header = %v", md)
	}
}

func TestClassify(t *testing.T) {
	err := classify(context.Background(), "op", errors.New("dial tcp: connection refused"))
	if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
		t.Fatalf("classify() = %v, want retryable Unavailable", err)
	}
}

func TestRunWithUnreachableRedis(t *testing.T) {
	m := miniredis.RunT(t)
	addr := m.Addr()
	m.Close()
	rdb := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	defer rdb.Close()
	q := NewFromClient(rdb, WithLogger(testLogger(slog.New(slog.DiscardHandler))))
	defer q.Close()
	err := q.Run(t.Context(), nil)
	if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
		t.Fatalf("Run() = %v, want retryable Unavailable", err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}); errorx.CodeOf(err) != errorx.Unavailable {
		t.Fatalf("Enqueue() = %v, want Unavailable", err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, jobqueue.WithUnique("k", 0)); errorx.CodeOf(err) != errorx.Unavailable {
		t.Fatalf("Enqueue(unique) = %v, want Unavailable", err)
	}
}

// TestRunReadyFailures checks that RunReady does not report readiness when it
// cannot start, so that a worker hook fails its Start.
func TestRunReadyFailures(t *testing.T) {
	m := miniredis.RunT(t)
	addr := m.Addr()
	m.Close()
	rdb := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	defer rdb.Close()

	t.Run("unreachable", func(t *testing.T) {
		q := NewFromClient(rdb, WithLogger(testLogger(slog.New(slog.DiscardHandler))))
		defer q.Close()
		err := q.RunReady(t.Context(), nil, func() { t.Error("ready called") })
		if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
			t.Fatalf("RunReady() = %v, want retryable Unavailable", err)
		}
	})
	t.Run("closed", func(t *testing.T) {
		q := NewFromClient(rdb, WithLogger(testLogger(slog.New(slog.DiscardHandler))))
		q.Close()
		if err := q.RunReady(t.Context(), nil, func() { t.Error("ready called") }); err != nil {
			t.Fatalf("RunReady() on a closed Queue = %v, want nil", err)
		}
	})
	t.Run("worker start", func(t *testing.T) {
		q := NewFromClient(rdb, WithLogger(testLogger(slog.New(slog.DiscardHandler))))
		defer q.Close()
		w := jobqueue.NewWorker(q, map[string]endpoint.Endpoint{})
		err := w.Start(t.Context())
		if errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatalf("worker Start() = %v, want Unavailable", err)
		}
		if err := w.Stop(t.Context()); err != nil {
			t.Fatalf("worker Stop() after failed Start = %v", err)
		}
	})
}

func TestConnectionLoss(t *testing.T) {
	b, m := miniBackend(t)
	q := b.newQueue(t, WithHealthCheckInterval(20*time.Millisecond))
	done := make(chan error, 1)
	go func() { done <- q.Run(t.Context(), nil) }()
	time.Sleep(100 * time.Millisecond)
	m.Close()
	select {
	case err := <-done:
		if errorx.CodeOf(err) != errorx.Unavailable || !errorx.IsRetryable(err) {
			t.Fatalf("Run() = %v, want retryable Unavailable", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Run did not report the connection loss")
	}
}

func TestConnectionLossFailsApp(t *testing.T) {
	b, m := miniBackend(t)
	q := b.newQueue(t, WithHealthCheckInterval(20*time.Millisecond))
	a := app.New(
		app.WithLogger(testLogger(slog.New(slog.DiscardHandler))),
		app.WithHooks(jobqueue.NewWorker(q, map[string]endpoint.Endpoint{})),
	)
	runErr := make(chan error, 1)
	go func() { runErr <- a.Run(t.Context()) }()
	time.Sleep(100 * time.Millisecond)
	m.Close()
	select {
	case err := <-runErr:
		if errorx.CodeOf(err) != errorx.Unavailable {
			t.Fatalf("Run() = %v, want Unavailable", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("App kept running after the connection was lost")
	}
	if s := a.State(); s != app.StateFailed {
		t.Fatalf("State() = %v, want failed", s)
	}
}

// Close of a Queue created with New must not close the Redis client while a
// Run is still shutting down its asynq server, whose subscriber panics on a
// closed connection.
func TestCloseWaitsForRunWithOwnedClient(t *testing.T) {
	m := miniredis.RunT(t)
	for range 5 {
		q, err := New(asynq.RedisClientOpt{Addr: m.Addr()}, WithLogger(testLogger(slog.New(slog.DiscardHandler))),
			WithShutdownTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- q.Run(t.Context(), nil) }()
		time.Sleep(100 * time.Millisecond)
		q.Close()
		// Close has finished the worker lifecycle. The wrapper goroutine may
		// still need to send the result after Run returns.
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run() after Close = %v, want nil", err)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Run did not return after Close")
		}
		if err := q.rdb.Ping(t.Context()).Err(); !errors.Is(err, redis.ErrClosed) {
			t.Fatalf("Ping() after Close = %v, want ErrClosed", err)
		}
		time.Sleep(50 * time.Millisecond) // let stray asynq goroutines run
	}
}

func TestStaleReservation(t *testing.T) {
	b, _ := miniBackend(t)
	q := b.newQueue(t)
	key := q.uniqueKey("k")
	unique := jobqueue.WithUnique("k", 0)

	// A provisional reservation, whose task is still being enqueued, holds.
	if err := q.rdb.Set(t.Context(), key, "enqueuing", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, unique); !errors.Is(err, jobqueue.ErrDuplicate) {
		t.Fatalf("Enqueue() during a provisional reservation = %v, want ErrDuplicate", err)
	}

	// A job-lifetime reservation of a job without task is over.
	if err := q.rdb.Set(t.Context(), key, "gone", 0).Err(); err != nil {
		t.Fatal(err)
	}
	id, err := q.Enqueue(t.Context(), &jobqueue.Job{Type: "work"}, unique, jobqueue.WithDelay(time.Hour))
	if err != nil {
		t.Fatalf("Enqueue() over a stale reservation = %v", err)
	}
	if v := q.rdb.Get(t.Context(), key).Val(); v != id {
		t.Fatalf("reservation = %q, want %q", v, id)
	}
	if ttl := q.rdb.TTL(t.Context(), key).Val(); ttl != -1 {
		t.Fatalf("TTL of the confirmed reservation = %v, want none", ttl)
	}

	// A provisional reservation that expired before it was confirmed is
	// made again.
	q.rdb.Del(t.Context(), key)
	q.confirm(t.Context(), key, id)
	if v := q.rdb.Get(t.Context(), key).Val(); v != id {
		t.Fatalf("reservation after confirm = %q, want %q", v, id)
	}
	// It is not taken from another job.
	q.confirm(t.Context(), key, "other")
	if v := q.rdb.Get(t.Context(), key).Val(); v != id {
		t.Fatalf("reservation after foreign confirm = %q, want %q", v, id)
	}
}

func TestHandlerPanicFailsJob(t *testing.T) {
	var fails failures
	q := NewFromClient(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), WithFailed(fails.record),
		WithLogger(testLogger(slog.New(slog.DiscardHandler))))
	defer q.rdb.Close()
	defer q.Close()
	h := q.handler(t.Context(), map[string]endpoint.Endpoint{"work": func(context.Context, any) (any, error) {
		panic("boom")
	}})
	err := h(t.Context(), asynq.NewTask("work", nil))
	if !errors.Is(err, asynq.SkipRetry) || errorx.CodeOf(err) != errorx.Internal {
		t.Fatalf("handler() = %v, want Internal with SkipRetry", err)
	}
	if fails.len() != 1 || errorx.IsRetryable(fails.errs[0]) {
		t.Fatalf("failures = %d, %v", fails.len(), fails.errs)
	}
}
