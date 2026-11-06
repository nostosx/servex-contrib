package database

import (
	"context"
	"database/sql"
	"errors"
)

// ErrNestedTransaction means the same Transactor already owns a transaction in
// the supplied context. Nested boundaries are rejected rather than silently
// joining the outer transaction. Propagate this error to roll back the outer call.
var ErrNestedTransaction = errors.New("database: nested transaction")

// DBTX is the native query/exec subset shared by *sql.DB and *sql.Tx. Repository
// adapters can depend on Transactor.Executor without knowing middleware or DI.
type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Transactor borrows a pool and supplies an explicit business transaction scope.
// It owns no pool lifecycle. Construct once and inject the same instance into
// transaction middleware and every participating repository. Do not copy it.
type Transactor struct {
	db      *sql.DB
	options sql.TxOptions
}

type transactionKey struct{ owner *Transactor }

// NewTransactor borrows db and copies options; nil options use SQL defaults.
// A nil pool panics during assembly. It performs no I/O or implicit migration.
func NewTransactor(db *sql.DB, options *sql.TxOptions) *Transactor {
	if db == nil {
		panic("database: nil transaction pool")
	}
	t := &Transactor{db: db}
	if options != nil {
		t.options = *options
	}
	return t
}

// Within implements the middleware transaction.Runner contract structurally.
// It delegates cleanup/error handling to Tx and never retries. Nesting the same
// Transactor is rejected. Other Transactors own independent transactions; this
// is not distributed atomicity. Callback contexts and executors must not escape
// this call or be retained by background work.
func (t *Transactor) Within(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		panic("database: nil transaction callback")
	}
	if _, ok := ctx.Value(transactionKey{t}).(*sql.Tx); ok {
		return ErrNestedTransaction
	}
	return Tx(ctx, t.db, &t.options, func(tx *sql.Tx) error {
		return fn(context.WithValue(ctx, transactionKey{t}, tx))
	})
}

// Join explicitly joins this Transactor's existing transaction or starts one.
// It creates no savepoint. Propagate callback errors to the outer owner so that
// it rolls back. Use when an idempotency store already owns the atomic boundary.
func (t *Transactor) Join(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		panic("database: nil transaction callback")
	}
	if _, ok := ctx.Value(transactionKey{t}).(*sql.Tx); ok {
		return fn(ctx)
	}
	return t.Within(ctx, fn)
}

// Executor returns this scope's transaction when present, otherwise its pool.
// Inside Within it never falls back to the pool, including after cancellation;
// SQL operations on a completed transaction fail with sql.ErrTxDone.
func (t *Transactor) Executor(ctx context.Context) DBTX {
	if tx, ok := ctx.Value(transactionKey{t}).(*sql.Tx); ok {
		return tx
	}
	return t.db
}
