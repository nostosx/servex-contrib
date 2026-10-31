// Package postgres provides explicit extension administration and shared SQL
// primitives. It never opens/closes a caller's pool or installs anything in New.
package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/nostosx/servex/errorx"
)

// DBTX is satisfied by *sql.DB and *sql.Tx. Adapters can join business transactions.
type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Labels preserves exact strings instead of JSON's replacement of invalid UTF-8.
// PostgreSQL JSONB cannot represent NUL, including escaped NUL.
func Labels(labels map[string]string) (string, error) {
	for key, value := range labels {
		if !utf8.ValidString(key) || !utf8.ValidString(value) || strings.ContainsRune(key, 0) || strings.ContainsRune(value, 0) {
			return "", Invalid("metadata labels must be UTF-8 without NUL")
		}
	}
	if labels == nil {
		return "{}", nil
	}
	data, err := json.Marshal(labels)
	return string(data), err
}

// Ident quotes one or more independently validated PostgreSQL identifiers.
func Ident(parts ...string) (string, error) {
	if len(parts) == 0 {
		return "", Invalid("empty identifier")
	}
	quoted := make([]string, len(parts))
	for i, part := range parts {
		if !identifier.MatchString(part) {
			return "", Invalid("invalid SQL identifier")
		}
		quoted[i] = `"` + part + `"`
	}
	return strings.Join(quoted, "."), nil
}

// Invalid creates a nonretryable configuration/input error.
func Invalid(message string) error {
	return errorx.New(errorx.InvalidArgument, "POSTGRES_ARGUMENT", message)
}

// Error classifies server errors without importing a particular SQL driver.
// The cause remains available to errors.Is/As. Unknown errors are not blindly retried.
func Error(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errorx.From(err)
	}
	type stateError interface{ SQLState() string }
	var state stateError
	code := errorx.Unknown
	if errors.As(err, &state) {
		switch s := state.SQLState(); {
		case strings.HasPrefix(s, "08"), s == "57P01", s == "57P02", s == "57P03":
			code = errorx.Unavailable
		case s == "40001", s == "40P01":
			code = errorx.Aborted
		case s == "23505":
			code = errorx.AlreadyExists
		case strings.HasPrefix(s, "22"), strings.HasPrefix(s, "23"):
			code = errorx.InvalidArgument
		case s == "42501":
			code = errorx.PermissionDenied
		case s == "42P01", s == "42883", s == "42704":
			code = errorx.FailedPrecondition
		}
	}
	if code == errorx.Unknown {
		var network net.Error
		switch {
		case errors.Is(err, driver.ErrBadConn), errors.As(err, &network):
			code = errorx.Unavailable
		case errors.Is(err, sql.ErrConnDone), errors.Is(err, sql.ErrTxDone):
			code = errorx.FailedPrecondition
		}
	}
	return errorx.New(code, "POSTGRES_OPERATION", "PostgreSQL operation failed").WithCause(err)
}

// Transaction executes fn once; retrying an entire business transaction is the caller's policy.
func Transaction(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	if db == nil || fn == nil {
		return Invalid("database and transaction callback are required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Error(err)
	}
	defer func() { _ = tx.Rollback() }() // Also release on panic without replacing it.
	if err := fn(tx); err != nil {
		if rollback := tx.Rollback(); rollback != nil && !errors.Is(rollback, sql.ErrTxDone) {
			return errors.Join(err, Error(rollback))
		}
		return err
	}
	return Error(tx.Commit())
}

// Extension reports server availability separately from database installation.
type Extension struct{ Name, AvailableVersion, InstalledVersion, Schema string }

// Client borrows a pool. Construction performs no I/O.
type Client struct{ db *sql.DB }

func New(db *sql.DB) (*Client, error) {
	if db == nil {
		return nil, Invalid("nil PostgreSQL database")
	}
	return &Client{db: db}, nil
}

// Extensions lists server-provided extensions, including installed ones.
func (c *Client) Extensions(ctx context.Context) ([]Extension, error) {
	rows, err := c.db.QueryContext(ctx, `SELECT a.name, a.default_version, COALESCE(e.extversion,''), COALESCE(n.nspname,'') FROM pg_available_extensions a LEFT JOIN pg_extension e ON e.extname=a.name LEFT JOIN pg_namespace n ON n.oid=e.extnamespace ORDER BY a.name`)
	if err != nil {
		return nil, Error(err)
	}
	defer rows.Close()
	var result []Extension
	for rows.Next() {
		var e Extension
		if err := rows.Scan(&e.Name, &e.AvailableVersion, &e.InstalledVersion, &e.Schema); err != nil {
			return nil, Error(err)
		}
		result = append(result, e)
	}
	return result, Error(rows.Err())
}

// Require checks installed extension names, without attempting installation.
func Require(ctx context.Context, db DBTX, names ...string) error {
	for _, name := range names {
		if _, err := Ident(name); err != nil {
			return err
		}
		var installed bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname=$1)`, name).Scan(&installed); err != nil {
			return Error(err)
		}
		if !installed {
			return errorx.Newf(errorx.FailedPrecondition, "POSTGRES_EXTENSION", "required PostgreSQL extension %s is not installed", name)
		}
	}
	return nil
}

// Enable explicitly installs an available extension. It does not use CASCADE,
// change preload settings, restart PostgreSQL, or download server packages.
func (c *Client) Enable(ctx context.Context, name string) error {
	quoted, err := Ident(name)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS "+quoted)
	return Error(err)
}

// Check implements health.Checker.
func (c *Client) Check(ctx context.Context) error { return Error(c.db.PingContext(ctx)) }

// Table identifies a relation without accepting raw SQL fragments.
type Table struct{ Schema, Name string }

func (t Table) SQL() (string, error) { return Ident(t.Schema, t.Name) }

// Regclass returns the fully quoted name suitable for a bound regclass argument.
func (t Table) Regclass() (string, error) { return t.SQL() }

// CreateHypertable explicitly converts an existing table using TimescaleDB.
func (c *Client) CreateHypertable(ctx context.Context, table Table, timeColumn string) error {
	t, err := table.SQL()
	if err != nil {
		return err
	}
	if _, err = Ident(timeColumn); err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `SELECT create_hypertable($1::regclass,$2::name,if_not_exists=>true)`, t, timeColumn)
	return Error(err)
}

// DistributeTable explicitly shards an existing relation using Citus.
func (c *Client) DistributeTable(ctx context.Context, table Table, column string) error {
	t, err := table.SQL()
	if err != nil {
		return err
	}
	if _, err = Ident(column); err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, `SELECT create_distributed_table($1::regclass,$2::text)`, t, column)
	return Error(err)
}

// ValidateJSON runs pg_jsonschema against a caller-supplied schema and document.
func (c *Client) ValidateJSON(ctx context.Context, schema, document []byte) (bool, error) {
	var valid bool
	err := c.db.QueryRowContext(ctx, `SELECT jsonb_matches_schema($1::json,$2::jsonb)`, string(schema), string(document)).Scan(&valid)
	return valid, Error(err)
}

// String is safe for diagnostics: no DSN, query or credentials are retained.
func (t Table) String() string { return fmt.Sprintf("%s.%s", t.Schema, t.Name) }
