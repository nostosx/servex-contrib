// Transaction middleware decorates typed application calls with an injected
// transaction boundary. It imports no database driver or concrete backend.
package application

import (
	"context"

	"github.com/nostosx/servex/application"
)

// TransactionRunner executes fn synchronously exactly once with a transaction-bearing
// context, commits on success and rolls back on error, panic or Goexit.
// It must preserve error causes and panic values and never replay fn. Repository
// adapters must use the transaction carried by the callback context.
type TransactionRunner interface {
	Within(context.Context, func(context.Context) error) error
}

// TransactionRunnerFunc adapts an explicit scope policy, such as Transactor.Join.
type TransactionRunnerFunc func(context.Context, func(context.Context) error) error

func (f TransactionRunnerFunc) Within(ctx context.Context, fn func(context.Context) error) error {
	return f(ctx, fn)
}

// Transaction wraps a call in runner. Only a successful commit publishes its result;
// failures return a zero result and the original runner error. No retries or
// panic recovery are added. A nil runner panics during configuration; typed nil
// runners are programming errors. Nested transaction policy belongs to runner.
func Transaction[Request, Result any](runner TransactionRunner) application.Middleware[Request, Result] {
	if runner == nil {
		panic("transaction: nil runner")
	}
	return func(_ application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, request Request) (Result, error) {
			var result Result
			err := runner.Within(ctx, func(txctx context.Context) error {
				var callErr error
				result, callErr = next.Handle(txctx, request)
				return callErr
			})
			if err != nil {
				var zero Result
				return zero, err
			}
			return result, nil
		})
	}
}
