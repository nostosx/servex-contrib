# goose

**v1.0.0 (2026-10-31).** Instance-scoped database migrations using community
github.com/pressly/goose/v3 v3.29.0 (MIT), Go 1.27.3. PostgreSQL acceptance uses
pgx v5.11.0. Production uses only Goose and standard library; pgx/sqlmock are tests.

## Usage

This complete, explicitly invoked migration program reads DATABASE_URL and
MIGRATIONS_DIR. It does not belong in every HTTP replica's startup by default.
The driver may be shared with GORM through its underlying *sql.DB.

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "os"
    "time"

    _ "github.com/jackc/pgx/v5/stdlib"
    sdk "github.com/pressly/goose/v3"
    "github.com/pressly/goose/v3/lock"

    "github.com/nostosx/servex-contrib/database/goose"
)

func run() error {
    db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
    if err != nil { return err }
    defer db.Close()
    locker, err := lock.NewPostgresSessionLocker()
    if err != nil { return err }
    migrator, err := goose.New(sdk.DialectPostgres, db,
        os.DirFS(os.Getenv("MIGRATIONS_DIR")), sdk.WithSessionLocker(locker))
    if err != nil { return err }
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()
    _, err = migrator.Up(ctx)
    return err
}

func main() { if err := run(); err != nil { log.Fatal(err) } }
```

For example, 00001_create.sql contains:

```sql
-- +goose Up
CREATE TABLE example (id bigint PRIMARY KEY);
-- +goose Down
DROP TABLE example;
```

Use embed.FS or fs.Sub instead of os.DirFS for embedded migrations. Up, UpTo,
Down and Status preserve native SDK results/errors. Construction performs no DB
I/O; the global Go-migration registry is always disabled. Register Go migrations
through provider options explicitly. The caller owns DB shutdown and migration
authorization. Down is an explicit potentially destructive operation.

PostgreSQL session locks require a connection that stays pinned for the run;
transaction-pooling proxies need a compatible deployment/locking design. Options
are not silently selected for other SQL dialects. A failed batch can follow
already committed migrations; inspect PartialError/history before retrying.

## Checks

`task check lint vulncheck MODULES=database/goose` from the root. Unit tests verify
construction does not execute SQL, context cancellation and invalid input. Real
PostgreSQL tests create a unique schema, run/repeat migrations, inspect status,
verify failed-migration rollback and explicitly exercise Down, then drop only
their own schema:

```sh
SERVEX_IT_POSTGRES_DSN='host=127.0.0.1 port=5432 user=servex password=servex dbname=servex_it sslmode=disable' task test INTEGRATION=true MODULES=database/goose
```

Use a disposable database. Tests do not prove portability to other SQL dialects.
