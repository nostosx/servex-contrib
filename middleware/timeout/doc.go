// Package timeout provides middleware that bounds a single call attempt with
// a deadline and reports an exceeded deadline as errorx.DeadlineExceeded.
//
// In a client chain it sits inside retry, so every attempt gets its own
// budget; the overall budget is the deadline of the caller's context.
package timeout
