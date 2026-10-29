// Package memory provides an in-process jobqueue.Enqueuer and
// jobqueue.Runner intended for tests, examples and single-process
// applications.
//
// Jobs are kept in memory only: delayed jobs and retries are scheduled with
// timers, and everything still pending is discarded by Close.
package memory
