package logging_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nostosx/servex-contrib/middleware/logging"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/logger"
)

func TestNilLoggerStillChoosesLevelAfterHandler(t *testing.T) {
	calls := 0
	handled := false
	cause := errorx.New(errorx.NotFound, "NOT_FOUND", "missing")
	ep := logging.New(nil, logging.WithLevel(func(code errorx.Code) logger.Level {
		calls++
		if !handled || code != errorx.NotFound {
			t.Fatalf("level callback before handler or wrong code: handled=%v code=%v", handled, code)
		}
		return logger.LevelWarn
	}))(func(ctx context.Context, req any) (any, error) {
		if ctx != t.Context() || req != "request" {
			t.Fatal("request/context changed")
		}
		handled = true
		return "response", cause
	})
	resp, err := ep(t.Context(), "request")
	if resp != "response" || err != cause || calls != 1 {
		t.Fatalf("response=%v error=%v level calls=%d", resp, err, calls)
	}
}

type delayedLogger struct{ duration time.Duration }

func (*delayedLogger) Enabled(context.Context, logger.Level) bool {
	time.Sleep(20 * time.Millisecond)
	return true
}

func (l *delayedLogger) Log(_ context.Context, _ logger.Level, _ string, attrs ...any) {
	for i := 0; i < len(attrs); i += 2 {
		if attrs[i] == logging.KeyDuration {
			l.duration = attrs[i+1].(time.Duration)
		}
	}
}

func TestDurationExcludesLevelAndEnabledWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &delayedLogger{}
		ep := logging.New(l, logging.WithLevel(func(errorx.Code) logger.Level {
			time.Sleep(10 * time.Millisecond)
			return logger.LevelInfo
		}))(func(context.Context, any) (any, error) {
			time.Sleep(150 * time.Millisecond)
			return nil, nil
		})
		if _, err := ep(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		if l.duration != 150*time.Millisecond {
			t.Fatalf("logged duration=%v, want handler duration only", l.duration)
		}
	})
}
