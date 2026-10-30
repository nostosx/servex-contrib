//go:build integration

package gorm_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
	"uuid"

	"gorm.io/driver/postgres"
	sdkgorm "gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/nostosx/servex-contrib/database/gorm"
	"github.com/nostosx/servex/app"
)

func TestIntegrationPostgres(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SERVEX_IT_POSTGRES_DSN not set")
	}
	db, err := sdkgorm.Open(postgres.Open(dsn), &sdkgorm.Config{DisableAutomaticPing: true, Logger: logger.Discard})
	if err != nil {
		t.Fatal(err)
	}
	hook, err := gorm.Hook("postgres", db)
	if err != nil {
		t.Fatal(err)
	}
	a := app.New(app.WithHooks(hook))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := a.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Check(ctx); err != nil {
		t.Fatal(err)
	}
	rec := newRecorder()
	if err := db.Use(gorm.NewPlugin(rec, rec)); err != nil {
		t.Fatal(err)
	}
	table := "servex_it_" + strings.ReplaceAll(uuid.NewV7().String(), "-", "")
	if err := db.WithContext(ctx).Table(table).AutoMigrate(&user{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.WithContext(ctx).Migrator().DropTable(table); err != nil {
			t.Error(err)
		}
	})
	u := user{Name: "alice"}
	if err := db.WithContext(ctx).Table(table).Create(&u).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.WithContext(ctx).Table(table).Model(&u).Update("name", "updated").Error; err != nil {
		t.Fatal(err)
	}
	errRollback := errors.New("rollback requested")
	err = db.WithContext(ctx).Transaction(func(tx *sdkgorm.DB) error {
		if err := tx.Table(table).Create(&user{Name: "rolled-back"}).Error; err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		t.Fatalf("transaction error: %v", err)
	}
	var users []user
	if err := db.WithContext(ctx).Table(table).Find(&users).Error; err != nil || len(users) != 1 || users[0].Name != "updated" {
		t.Fatalf("CRUD/rollback: %+v, %v", users, err)
	}
	if err := db.WithContext(ctx).Table(table).Delete(&u).Error; err != nil {
		t.Fatal(err)
	}
	if rec.counts["[postgres create ok]"] < 2 || rec.counts["[postgres query ok]"] < 1 || rec.counts["[postgres update ok]"] < 1 || rec.counts["[postgres delete ok]"] < 1 {
		t.Fatalf("missing real PostgreSQL telemetry: %v", rec.counts)
	}
	for _, s := range rec.spans {
		if s.ends != 1 {
			t.Fatalf("unfinished span: %+v", s)
		}
	}
}

func TestIntegrationPostgresResource(t *testing.T) {
	dsn := os.Getenv("SERVEX_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SERVEX_IT_POSTGRES_DSN not set")
	}
	rec := newRecorder()
	database, err := gorm.OpenPostgres("postgres", gorm.DefaultPostgresConfig(dsn), rec, rec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := database.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := database.Check(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := database.GORM().DB()
	if err != nil || pool != database.SQL() {
		t.Fatal("different connection pools", err)
	}
	var value int
	if err := database.GORM().WithContext(ctx).Raw("SELECT 42").Scan(&value).Error; err != nil || value != 42 {
		t.Fatal(value, err)
	}
	if err := database.SQL().QueryRowContext(ctx, "SELECT 7").Scan(&value); err != nil || value != 7 {
		t.Fatal(value, err)
	}
	if len(rec.spans) == 0 {
		t.Fatal("GORM observability missing")
	}
	if err := database.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Check(ctx); !errors.Is(err, gorm.ErrDatabaseClosed) {
		t.Fatal(err)
	}
}
