package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	sdk "github.com/hibiken/asynq"
	goredis "github.com/redis/go-redis/v9"
)

func TestSchedulerLifecycleAndValidation(t *testing.T) {
	server := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: server.Addr(), ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = client.Close() })
	s, err := NewScheduler(client, &sdk.SchedulerOpts{LogLevel: sdk.ErrorLevel}, []Schedule{{Spec: "@every 1h", Type: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(t.Context()); err == nil {
		t.Fatal("duplicate start")
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(t.Context()).Err(); err != nil {
		t.Fatal("closed injected client", err)
	}
	if _, err := NewScheduler(client, nil, []Schedule{{Spec: "invalid", Type: "work"}}); err == nil {
		t.Fatal("invalid schedule")
	}
	if _, err := NewScheduler(client, nil, []Schedule{{Spec: "@daily"}}); err == nil {
		t.Fatal("nil task")
	}
	stopped, err := NewScheduler(client, nil, []Schedule{{Spec: "@daily", Type: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := stopped.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stopped.Start(t.Context()); err == nil {
		t.Fatal("start after stop")
	}
}
