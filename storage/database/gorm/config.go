package gorm

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrInvalidDatabaseConfig identifies invalid connection, pool or runtime ownership settings.
var ErrInvalidDatabaseConfig = errors.New("gorm: invalid database configuration")

// DatabaseConfig contains file-decodable connection and pool settings. Start with
// DefaultDatabaseConfig before decoding overrides. Zero idle connections disables
// idle retention; zero durations disable the corresponding expiry. Native GORM
// objects are supplied through OpenPostgres options, never decoded from a file.
type DatabaseConfig struct {
	DSN             string        `json:"dsn"`
	MaxOpenConns    int           `json:"max_open_conns"`
	MaxIdleConns    int           `json:"max_idle_conns"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `json:"conn_max_idle_time"`
}

// DefaultDatabaseConfig returns pool defaults without choosing credentials or a
// destination. Set DSN directly or decode it from the application's configuration.
func DefaultDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{
		MaxOpenConns: 10, MaxIdleConns: 2,
		ConnMaxLifetime: 30 * time.Minute, ConnMaxIdleTime: 5 * time.Minute,
	}
}

// Validate checks PostgreSQL connection syntax and pool settings without network
// I/O. Errors identify fields but never include the DSN, credentials or raw pgx
// parse errors. Explicit zero idle and expiry values remain valid.
func (c DatabaseConfig) Validate() error {
	_, err := c.postgresConfig()
	return err
}

func (c DatabaseConfig) postgresConfig() (*pgx.ConnConfig, error) {
	switch {
	case strings.TrimSpace(c.DSN) == "":
		return nil, invalidDatabaseConfig("dsn")
	case c.MaxOpenConns <= 0:
		return nil, invalidDatabaseConfig("max_open_conns")
	case c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns:
		return nil, invalidDatabaseConfig("max_idle_conns")
	case c.ConnMaxLifetime < 0:
		return nil, invalidDatabaseConfig("conn_max_lifetime")
	case c.ConnMaxIdleTime < 0:
		return nil, invalidDatabaseConfig("conn_max_idle_time")
	}
	connection, err := pgx.ParseConfig(c.DSN)
	if err != nil {
		return nil, invalidDatabaseConfig("dsn")
	}
	return connection, nil
}

func invalidDatabaseConfig(field string) error {
	return fmt.Errorf("%w: %s", ErrInvalidDatabaseConfig, field)
}
