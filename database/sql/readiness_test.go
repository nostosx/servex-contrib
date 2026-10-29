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
	c := newFullConnector()
	db, _, _ := instrumented(t, c)
	h := Hook("primary", db, WithReadiness(false), nil)
	if _, ok := h.(interface{ Check(context.Context) error }); ok {
		t.Fatal("hook with readiness disabled implements Check")
	}
	// Start still verifies the connection.
	c.st.set(func(s *fakeState) { s.pingErr = errFake })
	if err := h.Start(t.Context()); !errors.Is(err, errFake) {
		t.Fatalf("Start err = %v, want errFake", err)
	}
	c.st.set(func(s *fakeState) { s.pingErr = nil })

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
