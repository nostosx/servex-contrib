// Package recovery provides middleware that turns panics raised by the next
// endpoint into errorx.Internal errors with reason "PANIC", logging the
// recovered value together with the stack trace.
package recovery
