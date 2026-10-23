// Package asynq implements jobqueue.Enqueuer and jobqueue.Runner on top of
// the Redis based task queue github.com/hibiken/asynq.
//
// A Job maps to an asynq task: Type is the task type, Payload the task
// payload, ID the task ID and Header the task headers; Attempt is derived
// from the retry count of the task. Delay and MaxRetry map to the asynq
// ProcessIn and MaxRetry options. Unique keys are reserved in Redis by the
// Queue itself, so that they follow the jobqueue semantics exactly. Jobs
// rejected with a non-retryable error are failed through asynq.SkipRetry and
// archived without further retries.
package asynq
