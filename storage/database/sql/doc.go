// Package database plugs database/sql into servex: it instruments drivers with
// tracing and metrics at the driver level, ties a *sql.DB to the application
// lifecycle and offers a transaction helper. Callers keep working with native
// *sql.DB and *sql.Tx values rather than database wrappers.
package database
