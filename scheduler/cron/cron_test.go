package cron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestLifecycleNoOverlapAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		started := make(chan struct{})
		finished := make(chan struct{})
		s, err := New(Config{Jobs: []Job{{Name: "slow", Spec: "@every 1s", Timeout: time.Minute, Run: func(ctx context.Context) error {
			calls.Add(1)
			close(started)
			<-ctx.Done()
			close(finished)
			return nil
		}}}})
		if err != nil {
			t.Fatal(err)
		}
		startup, cancel := context.WithCancel(t.Context())
		if err := s.Start(startup); err != nil {
			t.Fatal(err)
		}
		cancel()
		time.Sleep(time.Second)
		<-started
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatal("overlapping job")
		}
		if err := s.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		<-finished
		if err := s.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := s.Start(t.Context()); err == nil {
			t.Fatal("restart accepted")
		}
	})
}

func TestErrorsAndPanicRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reported := make(chan error, 2)
		want := errors.New("job failed")
		s, err := New(Config{Jobs: []Job{
			{Name: "error", Spec: "@every 1s", Timeout: time.Second, Run: func(context.Context) error { return want }},
			{Name: "panic", Spec: "@every 1s", Timeout: time.Second, Run: func(context.Context) error { panic("boom") }},
		}, OnError: func(_ string, err error) { reported <- err }})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		first, second := <-reported, <-reported
		if !errors.Is(first, want) && !errors.Is(second, want) {
			t.Fatal(first, second)
		}
		if err := s.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestValidation(t *testing.T) {
	for _, jobs := range [][]Job{nil, {{Name: "x", Spec: "invalid", Timeout: time.Second, Run: func(context.Context) error { return nil }}}, {{Name: "x", Spec: "@daily"}}} {
		if _, err := New(Config{Jobs: jobs}); err == nil {
			t.Fatal("invalid jobs accepted")
		}
	}
}
