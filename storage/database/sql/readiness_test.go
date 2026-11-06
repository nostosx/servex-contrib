package database

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"

	"github.com/nostosx/servex/app"
)

func TestHookWithoutReadiness(t *testing.T) {
	failedConnector := newFullConnector()
	failedDB, _, _ := instrumented(t, failedConnector)
	failedHook := Hook("unreachable", failedDB, WithReadiness(false), nil)
	if _, ok := failedHook.(interface{ Check(context.Context) error }); ok {
		t.Fatal("hook with readiness disabled implements Check")
	}
	// Startup still verifies and releases a pool that cannot start.
	failedConnector.st.set(func(s *fakeState) { s.pingErr = errFake })
	if err := failedHook.Start(t.Context()); !errors.Is(err, errFake) {
		t.Fatalf("Start err = %v, want errFake", err)
	}
	if !failedConnector.closed.Load() {
		t.Fatal("failed startup did not release its owned connector")
	}

	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	h := Hook("primary", db, WithReadiness(false))
	a := app.New(app.WithHooks(h), app.WithLogger(testLogger(slog.New(slog.DiscardHandler))))
	if err := a.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A database outage after start does not fail readiness.
	c.st.set(func(s *fakeState) { s.pingErr = errFake })
	if err := a.Check(t.Context()); err != nil {
		t.Errorf("App.Check = %v, want nil without database readiness", err)
	}
	c.st.set(func(s *fakeState) { s.pingErr = nil })
	if err := a.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !c.closed.Load() {
		t.Error("connector not closed by Stop")
	}
}

func TestHookWithReadinessEnabled(t *testing.T) {
	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	h := Hook("primary", db, WithReadiness(false), WithReadiness(true))
	checker, ok := h.(interface{ Check(context.Context) error })
	if !ok {
		t.Fatal("hook with readiness enabled does not implement Check")
	}
	c.st.set(func(s *fakeState) { s.pingErr = errFake })
	if err := checker.Check(t.Context()); !errors.Is(err, errFake) {
		t.Errorf("Check = %v, want errFake", err)
	}
	c.st.set(func(s *fakeState) { s.pingErr = nil })
	if err := h.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Metrics are recorded with the context carrying the operation's span, so a
// meter can link observations to traces.
func TestMetricsReceiveSpanContext(t *testing.T) {
	db, _, m := instrumented(t, newFullConnector())
	if _, err := db.ExecContext(t.Context(), "UPDATE t SET x = 1"); err != nil {
		t.Fatal(err)
	}
	spans := m.observedSpans()
	if len(spans) == 0 {
		t.Fatal("no metric observations")
	}
	if slices.Contains(spans, "") {
		t.Errorf("observation without the operation span; spans = %v", spans)
	}
	if !slices.Contains(spans, opExec) {
		t.Errorf("spans = %v, want the exec span", spans)
	}
}

func TestHookFailedStartupPreservesPingAndCleanupErrors(t *testing.T) {
	connector := newFullConnector()
	connector.st.pingErr = errFake
	closeErr := errors.New("connector cleanup failed")
	connector.closeErr = closeErr
	db, _, _ := instrumented(t, connector)
	owner := Hook("database", db)
	application := app.New(app.WithHooks(owner))
	if err := application.Start(t.Context()); !errors.Is(err, errFake) || !errors.Is(err, closeErr) {
		t.Fatal("failed startup lost ping or cleanup identity", err)
	}
	if !connector.closed.Load() {
		t.Fatal("failed startup left the owned connector open")
	}
	if err := db.PingContext(t.Context()); err == nil {
		t.Fatal("failed startup pool remains usable")
	}
}
