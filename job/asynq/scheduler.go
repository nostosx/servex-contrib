package asynq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	sdk "github.com/hibiken/asynq"
	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/app"
)

// Schedule is an SDK task and its enqueue options, repeated by a standard cron
// expression or descriptor. Configure one scheduler per schedule set; replicas
// do not elect a leader and can enqueue duplicates. Workers remain idempotent.
type Schedule struct {
	Spec    string
	Type    string
	Payload []byte
	Options []sdk.Option
}

// Scheduler adapts asynq's periodic enqueueing to app.Hook. The caller owns the
// Redis client. Configure finite SDK/client timeouts and short enqueue callbacks.
type Scheduler struct {
	mu      sync.Mutex
	engine  *sdk.Scheduler
	client  goredis.UniversalClient
	started bool
	stopped bool
	done    chan struct{}
}

var _ app.Hook = (*Scheduler)(nil)

func NewScheduler(client goredis.UniversalClient, options *sdk.SchedulerOpts, schedules []Schedule) (*Scheduler, error) {
	if client == nil || len(schedules) == 0 || len(schedules) > 1024 {
		return nil, errors.New("asynq: client and one to 1024 schedules required")
	}
	var copied sdk.SchedulerOpts
	if options != nil {
		copied = *options
	}
	// The SDK's shared-client Scheduler logs an error when Shutdown calls Close.
	// Give it a borrowed connection whose Close is an explicit no-op instead.
	engine := sdk.NewScheduler(schedulerRedisOption{client: client}, &copied)
	for _, schedule := range schedules {
		if schedule.Type == "" {
			return nil, errors.New("asynq: scheduled task type required")
		}
		task := sdk.NewTask(schedule.Type, append([]byte{}, schedule.Payload...))
		options := append([]sdk.Option{}, schedule.Options...)
		if _, err := engine.Register(schedule.Spec, task, options...); err != nil {
			return nil, fmt.Errorf("asynq: register schedule: %w", err)
		}
	}
	return &Scheduler{engine: engine, client: client, done: make(chan struct{})}, nil
}

type schedulerRedisOption struct{ client goredis.UniversalClient }

func (o schedulerRedisOption) MakeRedisClient() any { return schedulerRedis{o.client} }

type schedulerRedis struct{ goredis.UniversalClient }

func (schedulerRedis) Close() error { return nil }

func (s *Scheduler) Name() string { return "jobqueue/asynq/scheduler" }

func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if s.started || s.stopped {
		return errors.New("asynq: scheduler already started or stopped")
	}
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("asynq: scheduler readiness: %w", err)
	}
	if err := s.engine.Start(); err != nil {
		return err
	}
	s.started = true
	return nil
}

// Stop starts shutdown once. The upstream SDK cannot interrupt Shutdown with a
// context; ctx bounds waiting while its cleanup completes with client timeouts.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		if s.started {
			go func() { s.engine.Shutdown(); close(s.done) }()
		} else {
			close(s.done)
		}
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
