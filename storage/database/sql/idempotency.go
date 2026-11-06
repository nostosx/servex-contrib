package database

import (
	"bytes"
	"context"
	"errors"

	"github.com/nostosx/servex/storage/idempotency"
)

// PostgresIdempotency implements durable keyed execution using PostgreSQL row
// locks. It borrows the same Transactor injected into participating repositories.
// It creates no goroutines and performs no automatic schema migration or expiry.
type PostgresIdempotency struct{ scope *Transactor }

var _ idempotency.Store = (*PostgresIdempotency)(nil)

func NewPostgresIdempotency(scope *Transactor) *PostgresIdempotency {
	if scope == nil {
		panic("database: nil idempotency transaction scope")
	}
	return &PostgresIdempotency{scope: scope}
}

// EnsureSchema is an explicit migration. Run on a disposable fixture in tests
// and through the application's migration workflow in production.
func (s *PostgresIdempotency) EnsureSchema(ctx context.Context) error {
	_, err := s.scope.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS servex_idempotency (
 scope text NOT NULL, operation text NOT NULL, token text NOT NULL,
 fingerprint bytea NOT NULL, result bytea, completed boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (scope,operation,token))`)
	return err
}

// Do retains successful records without implicit expiry. Concurrent callers wait
// on the database lock, bounded by ctx. The callback and record share one SQL
// commit; repositories must use scope.Executor and nested decorators scope.Join.
// Remote side effects and other database scopes are outside this guarantee.
func (s *PostgresIdempotency) Do(ctx context.Context, key idempotency.Key, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, errors.New("database: nil idempotency callback")
	}
	var result []byte
	err := s.scope.Within(ctx, func(txctx context.Context) error {
		exec := s.scope.Executor(txctx)
		insert, err := exec.ExecContext(txctx, `INSERT INTO servex_idempotency(scope,operation,token,fingerprint) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, key.Scope, key.Operation, key.Token, key.Fingerprint[:])
		if err != nil {
			return err
		}
		created, err := insert.RowsAffected()
		if err != nil {
			return err
		}
		var digest []byte
		var completed bool
		if err := exec.QueryRowContext(txctx, `SELECT fingerprint,result,completed FROM servex_idempotency WHERE scope=$1 AND operation=$2 AND token=$3 FOR UPDATE`, key.Scope, key.Operation, key.Token).Scan(&digest, &result, &completed); err != nil {
			return err
		}
		if !bytes.Equal(digest, key.Fingerprint[:]) {
			return idempotency.ErrConflict
		}
		if completed {
			return nil
		}
		if created != 1 {
			return idempotency.ErrIncomplete
		}
		result, err = fn(txctx)
		if err != nil {
			return err
		}
		_, err = exec.ExecContext(txctx, `UPDATE servex_idempotency SET result=$4,completed=true WHERE scope=$1 AND operation=$2 AND token=$3`, key.Scope, key.Operation, key.Token, result)
		return err
	})
	if err != nil {
		return nil, err
	}
	return bytes.Clone(result), nil
}
