package observe_test

import (
	"context"
	"errors"
	"testing"

	clientmw "github.com/nostosx/servex-contrib/middleware/client"
	jobmw "github.com/nostosx/servex-contrib/middleware/job"
	messagingmw "github.com/nostosx/servex-contrib/middleware/messaging"

	"github.com/nostosx/servex/job"
	"github.com/nostosx/servex/messaging"
	"github.com/nostosx/servex/metadata"
	traceapi "github.com/nostosx/servex/observability/tracing"
	"github.com/nostosx/servex/transport"
)

func TestNativePublishAndEnqueuePreserveInputsAndOptions(t *testing.T) {
	r := &recording{}
	message := &messaging.Message{Topic: "orders", Header: metadata.Pairs("tenant", "a"), Body: []byte("payload")}
	publisher := messaging.DecoratePublisher(messaging.PublishFunc(func(_ context.Context, msg *messaging.Message) error {
		if msg == message || msg.ID == "" || msg.Header.Get("x-trace") == "" {
			t.Fatal("missing isolated publish metadata")
		}
		msg.Header.Set("tenant", "changed")
		msg.Body[0] = 'X'
		return nil
	}), messagingmw.Identity(), messagingmw.PublishTracing(r, propagator{}), messagingmw.PublishLogging(r), messagingmw.PublishMetrics(meter{r}))
	if err := publisher.Publish(t.Context(), message); err != nil {
		t.Fatal(err)
	}
	if message.ID != "" || message.Header.Get("tenant") != "a" || string(message.Body) != "payload" {
		t.Fatal("publisher mutated caller data")
	}
	work := &job.Job{Type: "refresh", Header: metadata.Pairs("tenant", "a"), Payload: []byte("payload")}
	enqueuer := job.DecorateEnqueuer(job.EnqueueFunc(func(_ context.Context, copy *job.Job, options ...job.EnqueueOption) (string, error) {
		settings := job.ApplyOptions(options...)
		if settings.MaxRetry != 7 || copy.Header.Get("x-trace") == "" {
			t.Fatal("native options or propagation lost")
		}
		copy.Header.Set("tenant", "changed")
		copy.Payload[0] = 'X'
		return "job-id", nil
	}), jobmw.EnqueueTracing(r, propagator{}), jobmw.EnqueueLogging(r), jobmw.EnqueueMetrics(meter{r}))
	if id, err := enqueuer.Enqueue(t.Context(), work, job.WithMaxRetry(7)); id != "job-id" || err != nil {
		t.Fatalf("enqueue=%q %v", id, err)
	}
	if work.Header.Get("tenant") != "a" || string(work.Payload) != "payload" {
		t.Fatal("enqueue mutated caller data")
	}
}

func TestProcessingAndSettlementOutcomesRemainSeparate(t *testing.T) {
	r := &recording{}
	consume := messaging.ChainConsume(messagingmw.ConsumeTracing(r, propagator{}), messagingmw.ConsumeLogging(r), messagingmw.ConsumeMetrics(meter{r}), messagingmw.Recovery())(func(ctx context.Context, msg *messaging.Message) error {
		if ctx.Value(extractedKey{}) != true {
			t.Fatal("message trace not extracted")
		}
		return nil
	})
	if err := consume(t.Context(), &messaging.Message{ID: "id", Topic: "orders"}); err != nil {
		t.Fatal(err)
	}
	if r.ended != 1 {
		t.Fatalf("processing spans=%d", r.ended)
	}
	eventCtx := traceapi.ContextWithSpan(t.Context(), r)
	event := messaging.Settlement{Topic: "orders", MessageID: "id", Action: "ack", Error: errors.New("ack failed")}
	messagingmw.SettlementLogging(r)(eventCtx, event)
	messagingmw.SettlementMetrics(meter{r})(eventCtx, event)
	messagingmw.SettlementTracing(r)(eventCtx, event)
	if r.ended != 2 {
		t.Fatal("settlement was not observed separately")
	}
	execute := job.ChainExecution(jobmw.ExecutionTracing(r, propagator{}), jobmw.ExecutionLogging(r), jobmw.ExecutionMetrics(meter{r}), jobmw.Recovery())(func(context.Context, *job.Job) error { panic("private payload") })
	if err := execute(t.Context(), &job.Job{ID: "job", Type: "refresh", Attempt: 2}); err == nil {
		t.Fatal("panic was not classified")
	}
}

func TestClientAttemptUsesNativeAddressAndPreservesError(t *testing.T) {
	r := &recording{}
	cause := errors.New("network failure")
	inv := clientmw.AttemptTracing(r, propagator{})(clientmw.AttemptLogging(r)(clientmw.AttemptMetrics(meter{r})(transport.InvokerFunc(func(_ context.Context, address string, request any) (any, error) {
		if address != "node:80" || request != 7 {
			t.Fatal("native invocation lost")
		}
		return 9, cause
	}))))
	ctx := metadata.NewContext(t.Context(), metadata.Info{Operation: "catalog.get"})
	result, err := inv.Invoke(transport.WithAttempt(ctx, transport.Attempt{Number: 2, Address: "node:80"}), "node:80", 7)
	if result != 9 || err != cause || r.ended != 1 {
		t.Fatalf("result=%v error=%v spans=%d", result, err, r.ended)
	}
}
