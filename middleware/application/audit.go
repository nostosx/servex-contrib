// Audit middleware records business outcomes through an application-owned recorder.
// Durable transactional recording requires a recorder that joins the call's
// transaction context. External sinks are not made atomic by this decorator.
package application

import (
	"context"
	"errors"
	"time"

	"github.com/nostosx/servex/application"
	"github.com/nostosx/servex/errorx"
)

// AuditEvent excludes request/result payloads, error messages and panic values.
// The recorder may read an authenticated actor from context.
type AuditEvent struct {
	Metadata    application.Metadata
	Code        errorx.Code
	Duration    time.Duration
	Interrupted bool
}
type AuditRecorder interface {
	Record(context.Context, AuditEvent) error
}
type AuditRecorderFunc func(context.Context, AuditEvent) error

func (f AuditRecorderFunc) Record(ctx context.Context, event AuditEvent) error { return f(ctx, event) }

// Audit records normal outcomes synchronously and fails closed if recording fails.
// Place inside the transaction decorator for an atomic SQL audit. It deliberately
// does not recover panics or replace them with recorder errors; interruption events
// are best-effort. A recorder must never panic.
func Audit[Request, Result any](recorder AuditRecorder) application.Middleware[Request, Result] {
	if recorder == nil {
		panic("audit: nil recorder")
	}
	return func(meta application.Metadata, next application.Handler[Request, Result]) application.Handler[Request, Result] {
		return application.Func[Request, Result](func(ctx context.Context, request Request) (result Result, err error) {
			start := time.Now()
			finished := false
			defer func() {
				if !finished {
					_ = recorder.Record(ctx, AuditEvent{Metadata: meta, Code: errorx.Internal, Duration: time.Since(start), Interrupted: true})
				}
			}()
			result, err = next.Handle(ctx, request)
			finished = true
			recordErr := recorder.Record(ctx, AuditEvent{Metadata: meta, Code: errorx.CodeOf(err), Duration: time.Since(start)})
			if recordErr != nil {
				var zero Result
				return zero, errors.Join(err, recordErr)
			}
			return result, err
		})
	}
}
