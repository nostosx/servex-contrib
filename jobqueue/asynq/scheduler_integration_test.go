//go:build integration

package asynq

import (
	"context"
	"os"
	"testing"
	"time"
	"uuid"

	sdk "github.com/hibiken/asynq"
	goredis "github.com/redis/go-redis/v9"
)

func TestIntegrationPeriodicScheduler(t *testing.T) {
	address := os.Getenv("SERVEX_IT_REDIS_ADDR")
	if address == "" {
		t.Skip("SERVEX_IT_REDIS_ADDR not set")
	}
	client := goredis.NewClient(&goredis.Options{Addr: address, ReadTimeout: time.Second, WriteTimeout: time.Second})
	t.Cleanup(func() { _ = client.Close() })
	queue := "servex-scheduler-" + uuid.NewV7().String()
	inspector := sdk.NewInspectorFromRedisClient(client)
	t.Cleanup(func() { _ = inspector.DeleteQueue(queue, true); _ = inspector.Close() })
	enqueued := make(chan *sdk.TaskInfo, 1)
	scheduler, err := NewScheduler(client, &sdk.SchedulerOpts{
		LogLevel: sdk.ErrorLevel,
		PostEnqueueFunc: func(info *sdk.TaskInfo, err error) {
			if err == nil {
				select {
				case enqueued <- info:
				default:
				}
			}
		},
	}, []Schedule{{Spec: "@every 1s", Type: "periodic", Payload: []byte("payload"), Options: []sdk.Option{sdk.Queue(queue), sdk.MaxRetry(0)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := scheduler.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := scheduler.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case info := <-enqueued:
		if info.Queue != queue || string(info.Payload) != "payload" {
			t.Fatal(info)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("periodic task was not enqueued")
	}
	if err := scheduler.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	tasks, err := inspector.ListPendingTasks(queue)
	if err != nil || len(tasks) == 0 {
		t.Fatalf("pending=%d err=%v", len(tasks), err)
	}
}
