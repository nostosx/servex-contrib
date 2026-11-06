package database

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Errors reproduced from the database/sql fallbacks for drivers that lack the
// context-aware interfaces.
var (
	errNamedParams    = errors.New("sql: driver does not support the use of Named Parameters")
	errIsolationLevel = errors.New("sql: driver does not support non-default isolation level")
	errReadOnly       = errors.New("sql: driver does not support read-only transactions")
)

// conn instruments a driver.Conn. It implements the interfaces whose absence
// database/sql treats like a pass-through implementation; the remaining
// optional interfaces live on the conn* types below and are attached by
// wrapConn only when the underlying connection implements them.
type conn struct {
	base driver.Conn
	in   *instrument
}

// Optional parts of conn, attached by wrapConn.
type (
	connExecer    conn // driver.ExecerContext
	connQueryer   conn // driver.QueryerContext
	connResetter  conn // driver.SessionResetter
	connValidator conn // driver.Validator
)

// Bits selecting the optional parts of a wrapped connection.
const (
	hasExecer = 1 << iota
	hasQueryer
	hasResetter
	hasValidator
)

// wrapConn instruments base, exposing exactly the optional interfaces of base
// that change the behaviour of database/sql.
func wrapConn(base driver.Conn, in *instrument) driver.Conn {
	c := &conn{base: base, in: in}
	var mask int
	switch base.(type) {
	case driver.ExecerContext, driver.Execer: //nolint:staticcheck // database/sql still uses legacy Execer drivers; the wrapper emulates ExecerContext on top
		mask |= hasExecer
	}
	switch base.(type) {
	case driver.QueryerContext, driver.Queryer: //nolint:staticcheck // database/sql still uses legacy Queryer drivers; the wrapper emulates QueryerContext on top
		mask |= hasQueryer
	}
	if _, ok := base.(driver.SessionResetter); ok {
		mask |= hasResetter
	}
	if _, ok := base.(driver.Validator); ok {
		mask |= hasValidator
	}

	type (
		E = driver.ExecerContext
		Q = driver.QueryerContext
		R = driver.SessionResetter
		V = driver.Validator
	)
	e, q, r, v := (*connExecer)(c), (*connQueryer)(c), (*connResetter)(c), (*connValidator)(c)
	switch mask {
	case hasExecer:
		return struct {
			*conn
			E
		}{c, e}
	case hasQueryer:
		return struct {
			*conn
			Q
		}{c, q}
	case hasExecer | hasQueryer:
		return struct {
			*conn
			E
			Q
		}{c, e, q}
	case hasResetter:
		return struct {
			*conn
			R
		}{c, r}
	case hasExecer | hasResetter:
		return struct {
			*conn
			E
			R
		}{c, e, r}
	case hasQueryer | hasResetter:
		return struct {
			*conn
			Q
			R
		}{c, q, r}
	case hasExecer | hasQueryer | hasResetter:
		return struct {
			*conn
			E
			Q
			R
		}{c, e, q, r}
	case hasValidator:
		return struct {
			*conn
			V
		}{c, v}
	case hasExecer | hasValidator:
		return struct {
			*conn
			E
			V
		}{c, e, v}
	case hasQueryer | hasValidator:
		return struct {
			*conn
			Q
			V
		}{c, q, v}
	case hasExecer | hasQueryer | hasValidator:
		return struct {
			*conn
			E
			Q
			V
		}{c, e, q, v}
	case hasResetter | hasValidator:
		return struct {
			*conn
			R
			V
		}{c, r, v}
	case hasExecer | hasResetter | hasValidator:
		return struct {
			*conn
			E
			R
			V
		}{c, e, r, v}
	case hasQueryer | hasResetter | hasValidator:
		return struct {
			*conn
			Q
			R
			V
		}{c, q, r, v}
	case hasExecer | hasQueryer | hasResetter | hasValidator:
		return struct {
			*conn
			E
			Q
			R
			V
		}{c, e, q, r, v}
	default:
		return c
	}
}

// Unwrap returns the driver's own connection, for use with sql.Conn.Raw.
func (c *conn) Unwrap() driver.Conn { return c.base }

// Prepare implements driver.Conn.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext implements driver.ConnPrepareContext, falling back to
// Prepare like database/sql when the driver lacks it.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	ctx, end := c.in.start(ctx, opPrepare, query)
	s, err := c.prepare(ctx, query)
	end(err)
	if err != nil {
		return nil, err
	}
	return wrapStmt(s, c, query), nil
}

func (c *conn) prepare(ctx context.Context, query string) (driver.Stmt, error) {
	if pc, ok := c.base.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	s, err := c.base.Prepare(query)
	if err == nil && ctx.Err() != nil {
		_ = s.Close()
		return nil, ctx.Err()
	}
	return s, err
}

// Close implements driver.Conn.
func (c *conn) Close() error { return c.base.Close() }

// Begin implements driver.Conn. database/sql calls BeginTx instead.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx, falling back to Begin like
// database/sql when the driver lacks it.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	parent := ctx
	ctx, end := c.in.start(ctx, opBegin, "")
	t, err := c.begin(ctx, opts)
	end(err)
	if err != nil {
		return nil, err
	}
	return &instrumentedTx{base: t, in: c.in, ctx: parent}, nil
}

func (c *conn) begin(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bc, ok := c.base.(driver.ConnBeginTx); ok {
		return bc.BeginTx(ctx, opts)
	}
	if opts.Isolation != 0 {
		return nil, errIsolationLevel
	}
	if opts.ReadOnly {
		return nil, errReadOnly
	}
	t, err := c.base.Begin() //nolint:staticcheck // fallback for legacy drivers
	if err == nil && ctx.Err() != nil {
		_ = t.Rollback()
		return nil, ctx.Err()
	}
	return t, err
}

// Ping implements driver.Pinger; it succeeds without contacting the database
// when the driver lacks it, as database/sql does.
func (c *conn) Ping(ctx context.Context) error {
	if p, ok := c.base.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

// CheckNamedValue implements driver.NamedValueChecker, returning
// driver.ErrSkip when the driver lacks it so that database/sql applies its
// default conversion.
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if nvc, ok := c.base.(driver.NamedValueChecker); ok {
		return nvc.CheckNamedValue(nv)
	}
	return driver.ErrSkip
}

// ExecContext implements driver.ExecerContext, falling back to driver.Execer.
func (c *connExecer) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ctx, end := c.in.start(ctx, opExec, query)
	res, err := c.exec(ctx, query, args)
	end(err)
	return res, err
}

func (c *connExecer) exec(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ec, ok := c.base.(driver.ExecerContext); ok {
		return ec.ExecContext(ctx, query, args)
	}
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.base.(driver.Execer).Exec(query, values) //nolint:staticcheck // fallback for legacy drivers
}

// QueryContext implements driver.QueryerContext, falling back to
// driver.Queryer.
func (c *connQueryer) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	ctx, end := c.in.start(ctx, opQuery, query)
	rows, err := c.query(ctx, query, args)
	end(err)
	return rows, err
}

func (c *connQueryer) query(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if qc, ok := c.base.(driver.QueryerContext); ok {
		return qc.QueryContext(ctx, query, args)
	}
	values, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.base.(driver.Queryer).Query(query, values) //nolint:staticcheck // fallback for legacy drivers
}

// ResetSession implements driver.SessionResetter.
func (c *connResetter) ResetSession(ctx context.Context) error {
	return c.base.(driver.SessionResetter).ResetSession(ctx)
}

// IsValid implements driver.Validator.
func (c *connValidator) IsValid() bool {
	return c.base.(driver.Validator).IsValid()
}

// namedValueToValue converts arguments for the legacy driver interfaces,
// rejecting named parameters like database/sql does.
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, len(named))
	for i, nv := range named {
		if nv.Name != "" {
			return nil, errNamedParams
		}
		values[i] = nv.Value
	}
	return values, nil
}

// instrumentedTx instruments a driver.Tx.
type instrumentedTx struct {
	base driver.Tx
	in   *instrument
	ctx  context.Context // context of BeginTx, used as the parent of the spans
}

// Commit implements driver.Tx.
func (t *instrumentedTx) Commit() error {
	_, end := t.in.start(t.ctx, opCommit, "")
	err := t.base.Commit()
	end(err)
	return err
}

// Rollback implements driver.Tx.
func (t *instrumentedTx) Rollback() error {
	_, end := t.in.start(t.ctx, opRollback, "")
	err := t.base.Rollback()
	end(err)
	return err
}
