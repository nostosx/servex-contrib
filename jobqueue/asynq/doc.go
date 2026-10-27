// Package asynq implements jobqueue.Enqueuer and jobqueue.ReadyRunner on top
// of the Redis based task queue github.com/hibiken/asynq. A jobqueue.NewWorker
// over a Queue starts only once Redis answered and the asynq server is
// processing the queue.
//
// A Job maps to an asynq task: Type is the task type, Payload the task
// payload, ID the task ID and Header the task headers; Attempt is derived
// from the retry count of the task. jobqueue.WithDelay and WithMaxRetry map to
// the asynq ProcessIn and MaxRetry options. Unique keys are reserved in Redis by the
// Queue itself, so that they follow the jobqueue semantics exactly. Jobs
// rejected with a non-retryable error are failed through asynq.SkipRetry and
// archived without further retries.
package asynq
