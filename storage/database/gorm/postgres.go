package gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	sdkgorm "gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

var ErrDatabaseClosed = errors.New("gorm: database resource closed")

// Database is one lifecycle owner for a GORM handle and its exact underlying
// SQL pool. Use GORM in repositories and SQL for migrations; do not register a
// second owning Hook for either handle. Construction creates no schema.
type Database struct {
	name     string
	orm      *sdkgorm.DB
	pool     *sql.DB
	once     sync.Once
	closed   atomic.Bool
	done     chan struct{}
	closeErr error
}

var _ app.Hook = (*Database)(nil)

// OpenPostgres configures the pool and installs observability once. Construction
// performs no connection ping. Start/Check ping with their operation context.
// Native GORM automatic ping is disabled; SQL logging defaults to Discard.
func OpenPostgres(name string, cfg DatabaseConfig, tracer tracing.Tracer, meter metrics.Meter, opts ...Option) (*Database, error) {
	if strings.TrimSpace(name) == "" {
		return nil, invalidDatabaseConfig("name")
	}
	connection, err := cfg.postgresConfig()
	if err != nil {
		return nil, err
	}
	options, err := newOptions(opts)
	if err != nil {
		return nil, err
	}
	return postgresResource(name, cfg, stdlib.OpenDB(*connection), tracer, meter, options.gorm)
}

func postgresResource(name string, cfg DatabaseConfig, pool *sql.DB, tracer tracing.Tracer, meter metrics.Meter, native *sdkgorm.Config) (resource *Database, err error) {
	// Also close when an application-supplied native plugin panics. Preserve its
	// panic while releasing the pool; normal constructor errors retain Close errors.
	defer func() {
		if resource == nil {
			err = errors.Join(err, pool.Close())
		}
	}()
	pool.SetMaxOpenConns(cfg.MaxOpenConns)
	pool.SetMaxIdleConns(cfg.MaxIdleConns)
	pool.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	pool.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	options := sdkgorm.Config{Logger: logger.Discard}
	if native != nil {
		options = *native
		options.Plugins = maps.Clone(native.Plugins)
		if options.Logger == nil {
			options.Logger = logger.Discard
		}
	}
	options.DisableAutomaticPing = true
	orm, err := sdkgorm.Open(postgres.New(postgres.Config{Conn: pool}), &options)
	if err == nil {
		err = orm.Use(NewPlugin(tracer, meter))
	}
	if err != nil {
		return nil, fmt.Errorf("gorm: initialize PostgreSQL: %w", err)
	}
	return &Database{name: name, orm: orm, pool: pool, done: make(chan struct{})}, nil
}

func (d *Database) Name() string      { return d.name }
func (d *Database) GORM() *sdkgorm.DB { return d.orm }
func (d *Database) SQL() *sql.DB      { return d.pool }

// Start verifies the owned pool and releases it if startup fails. App rolls back
// only hooks that already started; this hook must clean its own failed startup.
// ctx bounds cleanup waiting, while a later Close can join ongoing cleanup.
func (d *Database) Start(ctx context.Context) error {
	if err := d.Check(ctx); err != nil {
		return errors.Join(err, d.Stop(ctx))
	}
	return nil
}

func (d *Database) Check(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if d.closed.Load() {
		return ErrDatabaseClosed
	}
	if err := d.pool.PingContext(ctx); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return fmt.Errorf("gorm: PostgreSQL readiness: %w", err)
	}
	return nil
}

// Stop begins closing once, including before Start. ctx bounds waiting, while
// database/sql Close finishes independently because its API cannot be canceled.
// A later Stop or Close can wait for the same cleanup to finish.
func (d *Database) Stop(ctx context.Context) error {
	d.beginClose()
	select {
	case <-d.done:
		return d.closeErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Shutdown bridges lifecycle-aware dependency injectors to the same owner.
func (d *Database) Shutdown(ctx context.Context) error { return d.Stop(ctx) }

// Close is useful during construction rollback and waits for SQL pool cleanup.
func (d *Database) Close() error { d.beginClose(); <-d.done; return d.closeErr }

func (d *Database) beginClose() {
	d.once.Do(func() {
		d.closed.Store(true)
		go func() { d.closeErr = d.pool.Close(); close(d.done) }()
	})
}
