// Package fx bridges a native Uber Fx application to servex lifecycle and
// shutdown handling. Use native Fx constructors and providers in app.go; business
// constructors receive ordinary dependencies. Run keeps servex responsible for
// readiness, runtime failures, draining and shutdown, with Fx as the sole signal
// receiver. This package defines no dependency container or registration API.
package fx
