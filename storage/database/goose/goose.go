// Package goose adapts Goose's instance-scoped migration provider. Migrations
// run only when explicitly requested; constructing a Migrator never changes DBs.
package goose

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	sdk "github.com/pressly/goose/v3"
)

// Migrator uses an application-owned DB and migration filesystem. It never
// closes the DB. Configure a provider-specific locker for concurrent deploys.
type Migrator struct{ provider *sdk.Provider }

// New isolates migrations from Goose's global registry. Provider options retain
// native SDK behavior, including transaction directives and migration locking.
func New(dialect sdk.Dialect, db *sql.DB, migrations fs.FS, options ...sdk.ProviderOption) (*Migrator, error) {
	options = append(append([]sdk.ProviderOption{}, options...), sdk.WithDisableGlobalRegistry(true))
	provider, err := sdk.NewProvider(dialect, db, migrations, options...)
	if err != nil {
		return nil, fmt.Errorf("goose: configure: %w", err)
	}
	return &Migrator{provider: provider}, nil
}

// Up applies pending migrations. A failure may follow already committed
// migrations; preserve the SDK's PartialError and reconcile before retrying.
func (m *Migrator) Up(ctx context.Context) ([]*sdk.MigrationResult, error) {
	return m.provider.Up(ctx)
}

// UpTo applies pending migrations through the selected version.
func (m *Migrator) UpTo(ctx context.Context, version int64) ([]*sdk.MigrationResult, error) {
	return m.provider.UpTo(ctx, version)
}

// Down explicitly reverts one migration; it may delete application data.
func (m *Migrator) Down(ctx context.Context) (*sdk.MigrationResult, error) {
	return m.provider.Down(ctx)
}

// Status inspects applied and pending migrations using Goose's native results.
func (m *Migrator) Status(ctx context.Context) ([]*sdk.MigrationStatus, error) {
	return m.provider.Status(ctx)
}
