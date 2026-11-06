package database

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrInvalidDatabaseConfig identifies invalid file-decodable database settings.
var ErrInvalidDatabaseConfig = errors.New("database: invalid database configuration")

// DatabaseConfig contains the selected driver, connection string and pool
// settings. Start with DefaultDatabaseConfig before decoding overrides. Drivers
// are registered by the application; this module does not import a driver SDK.
// Zero idle connections disables idle retention; zero durations disable expiry.
type DatabaseConfig struct {
	Driver          string        `json:"driver"`
	DSN             string        `json:"dsn"`
	MaxOpenConns    int           `json:"max_open_conns"`
	MaxIdleConns    int           `json:"max_idle_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time"`
}

// DefaultDatabaseConfig returns bounded pool defaults, leaving driver and
// credentials for the application's configuration sources.
func DefaultDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{
		MaxOpenConns: 10, MaxIdleConns: 2,
		ConnMaxLifetime: 30 * time.Minute, ConnMaxIdleTime: 5 * time.Minute,
	}
}

// Validate checks required fields and pool limits without opening a driver.
// Driver-specific DSN validation happens during OpenFromConfig. Errors contain
// field names rather than values, so connection credentials are not disclosed.
func (c DatabaseConfig) Validate() error {
	var field string
	switch {
	case strings.TrimSpace(c.Driver) == "":
		field = "driver"
	case strings.TrimSpace(c.DSN) == "":
		field = "dsn"
	case c.MaxOpenConns <= 0:
		field = "max_open_conns"
	case c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns:
		field = "max_idle_conns"
	case c.ConnMaxLifetime < 0:
		field = "conn_max_lifetime"
	case c.ConnMaxIdleTime < 0:
		field = "conn_max_idle_time"
	default:
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInvalidDatabaseConfig, field)
}

// OpenFromConfig validates configuration, opens an instrumented pool and applies
// its limits. Like Open, it does not ping or register lifecycle ownership; callers
// must close the returned pool or register Hook. Runtime telemetry is supplied
// through opts, never configuration files. Driver errors retain errors.Is/As
// identity while their potentially sensitive text is omitted from Error().
func OpenFromConfig(cfg DatabaseConfig, opts ...Option) (*sql.DB, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	db, err := Open(cfg.Driver, cfg.DSN, opts...)
	if err != nil {
		return nil, &configurationOpenError{cause: err}
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	return db, nil
}

type configurationOpenError struct{ cause error }

func (*configurationOpenError) Error() string {
	return "database: invalid database configuration: open"
}
func (e *configurationOpenError) Unwrap() error      { return e.cause }
func (*configurationOpenError) Is(target error) bool { return target == ErrInvalidDatabaseConfig }
