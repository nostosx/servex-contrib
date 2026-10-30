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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	sdkgorm "gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/observability/metrics"
	"github.com/nostosx/servex/observability/tracing"
)

var (
	ErrInvalidPostgresConfig = errors.New("gorm: invalid PostgreSQL configuration")
	ErrDatabaseClosed        = errors.New("gorm: database resource closed")
)

// PostgresConfig configures one owned PostgreSQL pool. Start with
// DefaultPostgresConfig, then override pool limits. Zero durations disable the
// corresponding expiry; zero idle connections disables idle retention.
// GORM is shallow-snapshotted, including its Plugins map. Referenced callbacks,
// naming strategies and plugins must remain immutable and concurrency-safe.
type PostgresConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	GORM            *sdkgorm.Config
}

func DefaultPostgresConfig(dsn string) PostgresConfig {
	return PostgresConfig{
		DSN: dsn, MaxOpenConns: 10, MaxIdleConns: 2,
		ConnMaxLifetime: 30 * time.Minute, ConnMaxIdleTime: 5 * time.Minute,
	}
}

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
func OpenPostgres(name string, cfg PostgresConfig, tracer tracing.Tracer, meter metrics.Meter) (*Database, error) {
	if err := validatePostgres(name, cfg); err != nil {
		return nil, err
	}
	connection, err := pgx.ParseConfig(cfg.DSN)
	if err != nil {
		// Parse errors can include the DSN and credentials. Configuration errors
		// intentionally expose only the stable category, never raw input.
		return nil, ErrInvalidPostgresConfig
	}
	return postgresResource(name, cfg, stdlib.OpenDB(*connection), tracer, meter)
}

func validatePostgres(name string, cfg PostgresConfig) error {
	validPool := cfg.MaxOpenConns > 0 && cfg.MaxIdleConns >= 0 && cfg.MaxIdleConns <= cfg.MaxOpenConns
	validDurations := cfg.ConnMaxLifetime >= 0 && cfg.ConnMaxIdleTime >= 0
	if strings.TrimSpace(name) == "" || strings.TrimSpace(cfg.DSN) == "" || !validPool || !validDurations {
		return ErrInvalidPostgresConfig
	}
	if cfg.GORM != nil && (cfg.GORM.ConnPool != nil || cfg.GORM.Dialector != nil) {
		return fmt.Errorf("%w: external GORM connection pools/dialectors are not accepted by an owning constructor", ErrInvalidPostgresConfig)
	}
	return nil
}

func postgresResource(name string, cfg PostgresConfig, pool *sql.DB, tracer tracing.Tracer, meter metrics.Meter) (resource *Database, err error) {
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
	if cfg.GORM != nil {
		options = *cfg.GORM
		options.Plugins = maps.Clone(cfg.GORM.Plugins)
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

func (d *Database) Name() string                    { return d.name }
func (d *Database) GORM() *sdkgorm.DB               { return d.orm }
func (d *Database) SQL() *sql.DB                    { return d.pool }
func (d *Database) Start(ctx context.Context) error { return d.Check(ctx) }

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
