package database

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTransactorUsesSameTransactionAcrossRepositories(t *testing.T) {
	t.Parallel()
	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	db.SetMaxOpenConns(1)
	scope := NewTransactor(db, nil)
	other := NewTransactor(db, nil)
	if scope.Executor(t.Context()) != db {
		t.Fatal("outside a transaction repositories must use the pool")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var escaped context.Context
	err := scope.Within(ctx, func(txctx context.Context) error {
		escaped = txctx
		first, ok := scope.Executor(txctx).(*sql.Tx)
		if !ok || first != scope.Executor(txctx) || other.Executor(txctx) != db {
			t.Fatal("transaction identity or scope isolation lost")
		}
		// A mistaken pool fallback would wait for the only connection already
		// held by this transaction and fail the bounded caller context.
		for _, statement := range []string{"INSERT order", "INSERT audit"} {
			if _, err := scope.Executor(txctx).ExecContext(txctx, statement); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil || !c.st.has("commit") || c.st.has("rollback") {
		t.Fatalf("error=%v events=%v", err, c.st.events())
	}
	if _, err := scope.Executor(escaped).ExecContext(ctx, "INSERT late"); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("completed transaction must not fall back to pool: %v", err)
	}
}

func TestTransactorNestedErrorRollsBack(t *testing.T) {
	t.Parallel()
	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	scope := NewTransactor(db, nil)
	err := scope.Within(t.Context(), func(ctx context.Context) error {
		return scope.Within(ctx, func(context.Context) error {
			t.Fatal("nested callback must not run")
			return nil
		})
	})
	if !errors.Is(err, ErrNestedTransaction) || !c.st.has("rollback") || c.st.has("commit") {
		t.Fatalf("error=%v events=%v", err, c.st.events())
	}
}

func TestTransactorParallelScopes(t *testing.T) {
	t.Parallel()
	db, _, _ := instrumented(t, newFullConnector())
	scope := NewTransactor(db, nil)
	var wg sync.WaitGroup
	transactions := make(chan *sql.Tx, 8)
	for range cap(transactions) {
		wg.Go(func() {
			if err := scope.Within(t.Context(), func(ctx context.Context) error {
				tx, ok := scope.Executor(ctx).(*sql.Tx)
				if !ok {
					t.Error("missing transaction")
				}
				transactions <- tx
				return nil
			}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	close(transactions)
	seen := make(map[*sql.Tx]bool)
	for tx := range transactions {
		if tx == nil || seen[tx] {
			t.Fatal("concurrent calls shared a transaction")
		}
		seen[tx] = true
	}
}
