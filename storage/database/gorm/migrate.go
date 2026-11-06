package gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/driver/postgres"
	sdkgorm "gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// AutoMigrate reconciles PostgreSQL tables with native GORM models using a
// borrowed SQL pool. It creates no pool and never closes db. Models belong to
// the application or the adapter that owns their data. Call before serving.
// Changes run in one transaction under a database-wide advisory lock shared
// with Database.AutoMigrate; context cancellation bounds lock acquisition.
// GORM may add or alter columns, constraints and indexes, but does not drop
// obsolete columns. Destructive changes and data backfills remain explicit.
func AutoMigrate(ctx context.Context, db *sql.DB, models ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil {
		return errors.New("gorm: database is required")
	}
	orm, err := sdkgorm.Open(postgres.New(postgres.Config{Conn: db}), &sdkgorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Discard,
	})
	if err != nil {
		return fmt.Errorf("gorm: initialize migration: %w", err)
	}
	return autoMigrate(ctx, orm, models...)
}

// AutoMigrate reconciles models using this resource's native GORM configuration
// and owned pool. It has the same transaction and locking semantics as AutoMigrate.
func (d *Database) AutoMigrate(ctx context.Context, models ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.closed.Load() {
		return ErrDatabaseClosed
	}
	return autoMigrate(ctx, d.orm, models...)
}

func autoMigrate(ctx context.Context, db *sdkgorm.DB, models ...any) error {
	if len(models) == 0 {
		return nil
	}
	err := db.WithContext(ctx).Transaction(func(tx *sdkgorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended('nostosx.gorm.schema', 0))").Error; err != nil {
			return err
		}
		return tx.AutoMigrate(models...)
	})
	if err != nil {
		return fmt.Errorf("gorm: auto migrate: %w", err)
	}
	return nil
}
