// Package cron adapts robfig/cron to servex application lifecycle and contexts.
// It is an in-process scheduler, not a durable or distributed job queue.
package cron

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/robfig/cron/v3"

	"github.com/nostosx/servex/app"
)

// Job runs with a finite timeout. Jobs must honor cancellation. An invocation
// is skipped while the same job's preceding invocation is still running.
type Job struct {
	Name    string
	Spec    string
	Timeout time.Duration
	Run     func(context.Context) error
}

// Config selects immutable jobs, time zone and error handling. Jobs are bounded
// to 1024 entries. OnError runs synchronously and must return promptly, not panic.
type Config struct {
	Jobs     []Job
	Location *time.Location
	OnError  func(name string, err error)
}

type Scheduler struct {
	mu         sync.Mutex
	engine     *sdk.Cron
	cancel     context.CancelFunc
	runContext context.Context
	done       <-chan struct{}
	started    bool
	stopped    bool
	onError    func(string, error)
}

var _ app.Hook = (*Scheduler)(nil)

// New parses standard five-field expressions and descriptors such as @every.
// No goroutine starts before Start. Multiple replicas run independently.
func New(config Config) (*Scheduler, error) {
	if len(config.Jobs) == 0 || len(config.Jobs) > 1024 {
		return nil, errors.New("cron: between one and 1024 jobs required")
	}
	location := config.Location
	if location == nil {
		location = time.UTC
	}
	s := &Scheduler{engine: sdk.New(sdk.WithLocation(location)), onError: config.OnError}
	names := make(map[string]bool)
	for _, job := range config.Jobs {
		if job.Name == "" || names[job.Name] || job.Timeout <= 0 || job.Run == nil {
			return nil, errors.New("cron: unique names, positive timeout and job function required")
		}
		names[job.Name] = true
		var busy atomic.Bool
		_, err := s.engine.AddFunc(job.Spec, func() {
			if !busy.CompareAndSwap(false, true) {
				return
			}
			defer busy.Store(false)
			s.invoke(job)
		})
		if err != nil {
			return nil, fmt.Errorf("cron: job %s: %w", job.Name, err)
		}
	}
	return s, nil
}

func (s *Scheduler) Name() string { return "scheduler/cron" }

// Start owns a background lifetime independent of this startup context. Stop
// cancels it. Values are inherited, cancellation/deadlines intentionally are not.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if s.started || s.stopped {
		return errors.New("cron: scheduler already started or stopped")
	}
	s.runContext, s.cancel = context.WithCancel(context.WithoutCancel(ctx))
	s.started = true
	s.engine.Start()
	return nil
}

func (s *Scheduler) invoke(job Job) {
	ctx, cancel := context.WithTimeout(s.runContext, job.Timeout)
	defer cancel()
	defer func() {
		if recovered := recover(); recovered != nil {
			s.report(job.Name, fmt.Errorf("cron: job panicked: %v", recovered))
		}
	}()
	if err := context.Cause(ctx); err != nil {
		return
	}
	if err := job.Run(ctx); err != nil {
		s.report(job.Name, err)
	}
}

func (s *Scheduler) report(name string, err error) {
	if s.onError != nil {
		s.onError(name, err)
	}
}

// Stop prevents new ticks, cancels jobs and waits within ctx. If a job ignores
// cancellation, Stop may time out; a later Stop can finish waiting for that job.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		if s.cancel != nil {
			s.cancel()
		}
		s.done = s.engine.Stop().Done()
	}
	done := s.done
	s.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
