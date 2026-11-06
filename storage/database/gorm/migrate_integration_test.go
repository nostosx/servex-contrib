//go:build integration

package gorm_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/storage/database/gorm"
)

type migrateInitial struct {
	ID   string `gorm:"type:text;primaryKey"`
	Name string `gorm:"type:text;not null"`
}

func (migrateInitial) TableName() string { return "servex_auto_migrate_test" }

type migrateExpanded struct {
	ID   string `gorm:"type:text;primaryKey"`
	Name string `gorm:"type:text;not null;index:servex_auto_migrate_name"`
	Note string `gorm:"type:text;not null;default:''"`
}

func (migrateExpanded) TableName() string { return "servex_auto_migrate_test" }

func TestIntegrationAutoMigrate(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SERVEX_IT_POSTGRES_DSN not set")
	}
	cfg := gorm.DefaultDatabaseConfig()
	cfg.DSN = dsn
	db, err := gorm.OpenPostgres("auto-migrate", cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.GORM().WithContext(ctx).Migrator().DropTable(&migrateInitial{}); err != nil {
			t.Error(err)
		}
	})
	if err := db.AutoMigrate(t.Context(), &migrateInitial{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(t.Context(), "INSERT INTO servex_auto_migrate_test(id,name) VALUES('existing','original')"); err != nil {
		t.Fatal(err)
	}
	// Both owning and borrowed-pool APIs reconcile the same schema and preserve rows.
	for range 2 {
		if err := gorm.AutoMigrate(t.Context(), db.SQL(), &migrateExpanded{}); err != nil {
			t.Fatal(err)
		}
	}
	var name, note string
	if err := db.SQL().QueryRowContext(t.Context(), "SELECT name,note FROM servex_auto_migrate_test WHERE id='existing'").Scan(&name, &note); err != nil || name != "original" || note != "" {
		t.Fatal(name, note, err)
	}
	errorsCh := make(chan error, 4)
	for range 4 {
		go func() { errorsCh <- db.AutoMigrate(t.Context(), &migrateExpanded{}) }()
	}
	for range 4 {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	if !db.GORM().Migrator().HasIndex(&migrateExpanded{}, "servex_auto_migrate_name") {
		t.Fatal("missing declared index")
	}
	// Lock waiting must honor cancellation and leave the pool usable for a retry.
	tx, err := db.SQL().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), "SELECT pg_advisory_xact_lock(hashtextextended('nostosx.gorm.schema', 0))"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := db.AutoMigrate(ctx, &migrateExpanded{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(t.Context(), &migrateInitial{}); err != nil {
		t.Fatal(err)
	}
	if !db.GORM().Migrator().HasColumn(&migrateExpanded{}, "note") {
		t.Fatal("obsolete column was dropped")
	}
}
