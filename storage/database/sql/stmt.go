package database

import (
	"context"
	"database/sql/driver"
)

// stmt instruments a driver.Stmt. It always implements
// driver.StmtExecContext, driver.StmtQueryContext and
// driver.NamedValueChecker, reproducing the database/sql fallbacks when the
// driver lacks them; driver.ColumnConverter is attached by wrapStmt only when
// the underlying statement implements it.
type stmt struct {
	base  driver.Stmt
	conn  *conn
	query string
}

// stmtConverter is the driver.ColumnConverter part of stmt.
type stmtConverter stmt

func wrapStmt(base driver.Stmt, c *conn, query string) driver.Stmt {
	s := &stmt{base: base, conn: c, query: query}
	if _, ok := base.(driver.ColumnConverter); ok { //nolint:staticcheck // must mirror the driver
		// The alias gives the embedded field a name other than its method's;
		// a field named ColumnConverter would shadow the promoted method.
		type converter = driver.ColumnConverter //nolint:staticcheck // must mirror the driver
		return struct {
			*stmt
			converter
		}{s, (*stmtConverter)(s)}
	}
	return s
}

// Close implements driver.Stmt.
func (s *stmt) Close() error { return s.base.Close() }

// NumInput implements driver.Stmt.
func (s *stmt) NumInput() int { return s.base.NumInput() }

// Exec implements driver.Stmt. database/sql calls ExecContext instead.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valueToNamedValue(args))
}

// Query implements driver.Stmt. database/sql calls QueryContext instead.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valueToNamedValue(args))
}

// ExecContext implements driver.StmtExecContext.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	ctx, end := s.conn.in.start(ctx, opExec, s.query)
	res, err := s.exec(ctx, args)
	end(err)
	return res, err
}

func (s *stmt) exec(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if ec, ok := s.base.(driver.StmtExecContext); ok {
		return ec.ExecContext(ctx, args)
	}
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.base.Exec(values) //nolint:staticcheck // fallback for legacy drivers
}

// QueryContext implements driver.StmtQueryContext.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	ctx, end := s.conn.in.start(ctx, opQuery, s.query)
	rows, err := s.queryRows(ctx, args)
	end(err)
	return rows, err
}

func (s *stmt) queryRows(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if qc, ok := s.base.(driver.StmtQueryContext); ok {
		return qc.QueryContext(ctx, args)
	}
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.base.Query(values) //nolint:staticcheck // fallback for legacy drivers
}

// CheckNamedValue implements driver.NamedValueChecker. Like database/sql it
// prefers the statement's checker over the connection's, and returns
// driver.ErrSkip when neither exists so that database/sql continues with the
// column converter or its default conversion.
func (s *stmt) CheckNamedValue(nv *driver.NamedValue) error {
	if nvc, ok := s.base.(driver.NamedValueChecker); ok {
		return nvc.CheckNamedValue(nv)
	}
	return s.conn.CheckNamedValue(nv)
}

// ColumnConverter implements driver.ColumnConverter.
func (s *stmtConverter) ColumnConverter(idx int) driver.ValueConverter {
	return s.base.(driver.ColumnConverter).ColumnConverter(idx) //nolint:staticcheck // must mirror the driver
}

// valueToNamedValue converts positional arguments to ordinal named values.
func valueToNamedValue(values []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(values))
	for i, v := range values {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return named
}
