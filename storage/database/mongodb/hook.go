package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/nostosx/servex/app"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/health"
)

// Reasons attached to the errorx errors returned by the Hook.
const (
	// ReasonUnavailable marks a failed ping: the deployment could not be
	// reached or did not answer. It is retryable.
	ReasonUnavailable = "MONGODB_UNAVAILABLE"
	// ReasonDisconnected marks a ping on a disconnected client. It is
	// reported as errorx.FailedPrecondition and is not retryable: the client
	// will never reconnect.
	ReasonDisconnected = "MONGODB_CLIENT_DISCONNECTED"
)

// Hook returns an app.Hook named name for c. Start pings the deployment with
// the client's read preference, failing the start if it is unreachable; Stop
// disconnects c, waiting for in-use connections within the stop context. A
// client that is already disconnected stops without error. The returned
// value also implements Check(ctx) error, which pings the deployment, so it
// takes part in app.App readiness and can be used as a health.Checker.
//
// A failed ping is reported as a retryable errorx.Unavailable with the driver
// error as its cause, except that a failure caused by ctx ending keeps the
// context error (errorx.From reports Canceled or DeadlineExceeded) and a ping
// on a disconnected client is a non-retryable errorx.FailedPrecondition.
// Hook panics if c is nil.
func Hook(name string, c *mongo.Client) app.Hook {
	if c == nil {
		panic("mongodb: Hook with nil *mongo.Client")
	}
	return &clientHook{name: name, client: c}
}

// clientHook implements app.Hook and the optional Check method.
type clientHook struct {
	name   string
	client *mongo.Client
}

var (
	_ app.Hook       = (*clientHook)(nil)
	_ health.Checker = (*clientHook)(nil)
)

func (h *clientHook) Name() string { return h.name }

func (h *clientHook) Start(ctx context.Context) error { return h.Check(ctx) }

func (h *clientHook) Stop(ctx context.Context) error {
	if err := h.client.Disconnect(ctx); err != nil && !errors.Is(err, mongo.ErrClientDisconnected) {
		return fmt.Errorf("mongodb: disconnect %q: %w", h.name, err)
	}
	return nil
}

// Check pings the deployment.
func (h *clientHook) Check(ctx context.Context) error {
	err := h.client.Ping(ctx, nil)
	if err == nil {
		return nil
	}
	msg := fmt.Sprintf("mongodb: ping %q", h.name)
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return fmt.Errorf("%s: %w", msg, err)
	}
	if errors.Is(err, mongo.ErrClientDisconnected) {
		return errorx.New(errorx.FailedPrecondition, ReasonDisconnected, msg+": client is disconnected").
			WithCause(err)
	}
	return errorx.New(errorx.Unavailable, ReasonUnavailable, msg).WithCause(err)
}
