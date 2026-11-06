//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func testAudit(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "pgaudit")
	f := ownCapability(t, db)
	container := ownedContainer(t)
	f.exec(t, "SET pgaudit.log='read,write,ddl'")
	f.exec(t, "CREATE TABLE "+f.table("audit_probe")+" (id int PRIMARY KEY)")
	f.exec(t, "INSERT INTO "+f.table("audit_probe")+" VALUES(1)")
	_ = f.integer(t, "SELECT count(*) FROM "+f.table("audit_probe"))
	waitCapability(t, f.ctx, func() (bool, error) {
		out, err := exec.CommandContext(f.ctx, "docker", "logs", container).CombinedOutput()
		if err != nil {
			return false, err
		}
		read, write, ddl := false, false, false
		for line := range strings.SplitSeq(string(out), "\n") {
			if strings.Contains(line, "AUDIT:") && strings.Contains(line, f.schema) {
				read = read || strings.Contains(line, ",READ,")
				write = write || strings.Contains(line, ",WRITE,")
				ddl = ddl || strings.Contains(line, ",DDL,")
			}
		}
		return read && write && ddl, nil
	})
}

func ownedContainer(t *testing.T) string {
	t.Helper()
	name := os.Getenv("SERVEX_IT_POSTGRES_CONTAINER")
	if !strings.HasPrefix(name, "servex-") {
		t.Fatal("SERVEX_IT_POSTGRES_CONTAINER must identify the owned acceptance container")
	}
	return name
}

func testRepack(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "pg_repack")
	f := ownCapability(t, db)
	container := ownedContainer(t)
	f.exec(t, "CREATE TABLE "+f.table("repack_probe")+" (id int PRIMARY KEY,body text,epoch bigint NOT NULL DEFAULT 0) WITH(autovacuum_enabled=false)")
	f.exec(t, "INSERT INTO "+f.table("repack_probe")+" (id,body) SELECT x,repeat('x',500) FROM generate_series(1,20000) x")
	f.exec(t, "UPDATE "+f.table("repack_probe")+" SET body=repeat('y',700)")
	before := f.integer(t, "SELECT pg_relation_filenode($1::regclass)", f.table("repack_probe"))
	command := exec.CommandContext(f.ctx, "docker", "exec", container, "/usr/lib/postgresql/17/bin/pg_repack", "--dbname=servex_it", "--username=servex", "--table="+f.schema+".repack_probe", "--no-order")
	var output strings.Builder
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	writerDone := make(chan error, 1)
	var writes atomic.Int64
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				writerDone <- nil
				return
			case <-ticker.C:
				if _, err := db.ExecContext(f.ctx, "UPDATE "+f.table("repack_probe")+" SET epoch=epoch+1 WHERE id=1"); err != nil {
					writerDone <- err
					return
				}
				writes.Add(1)
			}
		}
	}()
	err := command.Wait()
	close(stop)
	writerErr := <-writerDone
	if err != nil {
		t.Fatalf("repack: %v: %s", err, output.String())
	}
	if writerErr != nil {
		t.Fatal(writerErr)
	}
	after := f.integer(t, "SELECT pg_relation_filenode($1::regclass)", f.table("repack_probe"))
	if before == after {
		t.Fatal("repack did not replace the physical table")
	}
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("repack_probe")); n != 20000 {
		t.Fatalf("repack row count %d", n)
	}
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("repack_probe")+" WHERE body<>repeat('y',700)"); n != 0 {
		t.Fatal("repack changed row contents")
	}
	if n := f.integer(t, "SELECT epoch FROM "+f.table("repack_probe")+" WHERE id=1"); n != writes.Load() || n < 1 {
		t.Fatalf("online writes lost: stored %d, committed %d", n, writes.Load())
	}
}

func testAsyncHTTP(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "pg_net")
	f := ownCapability(t, db)
	base := os.Getenv("SERVEX_IT_POSTGRES_HTTP_URL")
	if base == "" {
		t.Fatal("explicit owned HTTP fixture URL is required")
	}
	tx, err := f.conn.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := tx.QueryRowContext(f.ctx, `SELECT net.http_post(url:=$1,body:=$2::jsonb,timeout_milliseconds:=2000)`, base+"/echo", `{"nonce":"`+f.schema+`"}`).Scan(&id); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(f.ctx, "SELECT count(*) FROM net._http_response WHERE id=$1", id).Scan(&count); err != nil || count != 0 {
		_ = tx.Rollback()
		t.Fatalf("request executed before commit: %d %v", count, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	response := func(id int64, status int64) {
		t.Helper()
		waitCapability(t, f.ctx, func() (bool, error) {
			var actual int64
			var content string
			var failure sql.NullString
			err := f.conn.QueryRowContext(f.ctx, "SELECT status_code,COALESCE(content,''),error_msg FROM net._http_response WHERE id=$1", id).Scan(&actual, &content, &failure)
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if actual != status || failure.Valid {
				return false, errors.New("async HTTP status or transport failure")
			}
			if status == 200 && !strings.Contains(content, f.schema) {
				return false, errors.New("HTTP fixture did not echo the submitted nonce")
			}
			return true, nil
		})
	}
	response(id, 200)
	statusID := f.integer(t, `SELECT net.http_post(url:=$1,body:='{}'::jsonb,timeout_milliseconds:=2000)`, base+"/status/503")
	response(statusID, 503)
	tx, err = f.conn.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var rolledBack int64
	if err := tx.QueryRowContext(f.ctx, `SELECT net.http_post(url:=$1,body:='{}'::jsonb)`, base+"/echo").Scan(&rolledBack); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := f.integer(t, "SELECT count(*) FROM net.http_request_queue WHERE id=$1", rolledBack); n != 0 {
		t.Fatal("rolled back request remained queued")
	}
	if n := f.integer(t, "SELECT count(*) FROM net._http_response WHERE id=$1", rolledBack); n != 0 {
		t.Fatal("rolled back request executed")
	}
	f.exec(t, "DELETE FROM net._http_response WHERE id=$1 OR id=$2", id, statusID)
}

func testForeignDatabase(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "postgres_fdw")
	f := ownCapability(t, db)
	dsn := os.Getenv("SERVEX_IT_POSTGRES_PEER_DSN")
	hostname := os.Getenv("SERVEX_IT_POSTGRES_PEER_HOST")
	if dsn == "" || hostname == "" {
		t.Fatal("separate owned peer DSN and internal hostname required")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid peer DSN")
	}
	peer, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := peer.ExecContext(f.ctx, "CREATE SCHEMA "+f.quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		remote, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Error(err)
			return
		}
		defer remote.Close()
		if _, err := remote.ExecContext(cleanup, "DROP SCHEMA "+f.quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	if _, err := peer.ExecContext(f.ctx, "CREATE TABLE "+f.table("events")+" (id int PRIMARY KEY,body text); INSERT INTO "+f.table("events")+" VALUES(1,'remote')"); err != nil {
		t.Fatal(err)
	}
	server := f.schema + "_server"
	statement := f.text(t, "SELECT format($1::text,$2::text,$3::text,$4::text,$5::text)", `CREATE SERVER %I FOREIGN DATA WRAPPER postgres_fdw OPTIONS(host %L,dbname %L,port %L)`, server, hostname, cfg.Database, "5432")
	f.exec(t, statement)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, `DROP SERVER "`+server+`" CASCADE`); err != nil {
			t.Error(err)
		}
	})
	statement = f.text(t, "SELECT format($1::text,$2::text,$3::text,$4::text)", `CREATE USER MAPPING FOR CURRENT_USER SERVER %I OPTIONS(user %L,password %L)`, server, cfg.User, cfg.Password)
	f.exec(t, statement)
	target := f.schema + "_remote"
	quoted, _ := Ident(target)
	f.exec(t, "CREATE SCHEMA "+quoted)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = db.ExecContext(cleanup, "DROP SCHEMA "+quoted+" CASCADE")
	})
	f.exec(t, "IMPORT FOREIGN SCHEMA "+f.quoted+` FROM SERVER "`+server+`" INTO `+quoted)
	foreign := quoted + `."events"`
	if body := f.text(t, "SELECT body FROM "+foreign+" WHERE id=1"); body != "remote" {
		t.Fatal("remote read failed")
	}
	f.exec(t, "INSERT INTO "+foreign+" VALUES(2,'committed')")
	var count int
	if err := peer.QueryRowContext(f.ctx, "SELECT count(*) FROM "+f.table("events")+" WHERE id=2").Scan(&count); err != nil || count != 1 {
		t.Fatalf("remote commit %d: %v", count, err)
	}
	tx, err := f.conn.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(f.ctx, "INSERT INTO "+foreign+" VALUES(3,'rollback')"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := peer.QueryRowContext(f.ctx, "SELECT count(*) FROM "+f.table("events")+" WHERE id=3").Scan(&count); err != nil || count != 0 {
		t.Fatalf("remote rollback %d: %v", count, err)
	}
	if plan := f.plan(t, "SELECT * FROM "+foreign+" WHERE id=1"); !strings.Contains(plan, "Remote SQL:") || !strings.Contains(plan, "WHERE") {
		t.Fatalf("foreign predicate not pushed down: %s", plan)
	}
}

func testTimescale(t *testing.T, db *sql.DB, c *Client) {
	requireCapability(t, db, "timescaledb")
	f := ownCapability(t, db)
	relation := Table{Schema: f.schema, Name: "events"}
	f.exec(t, "CREATE TABLE "+f.table("events")+" (occurred_at timestamptz NOT NULL,tenant text,value int)")
	if err := c.CreateHypertable(f.ctx, relation, "occurred_at"); err != nil {
		t.Fatal(err)
	}
	f.exec(t, "SELECT set_chunk_time_interval($1::regclass,interval '1 day')", f.table("events"))
	f.exec(t, "INSERT INTO "+f.table("events")+" SELECT '2026-01-01'::timestamptz+x*interval '1 day','tenant',x+1 FROM generate_series(0,9) x")
	if n := f.integer(t, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema=$1", f.schema); n < 2 {
		t.Fatal("hypertable did not create time chunks")
	}
	f.exec(t, "CREATE MATERIALIZED VIEW "+f.table("daily")+" WITH(timescaledb.continuous) AS SELECT time_bucket(interval '1 day',occurred_at) AS bucket,tenant,sum(value) AS total FROM "+f.table("events")+" GROUP BY bucket,tenant WITH NO DATA")
	refresh := func() {
		f.exec(t, "CALL refresh_continuous_aggregate($1::regclass,NULL::timestamptz,NULL::timestamptz)", f.table("daily"))
	}
	refresh()
	if n := f.integer(t, "SELECT sum(total) FROM "+f.table("daily")); n != 55 {
		t.Fatalf("continuous aggregate %d", n)
	}
	f.exec(t, "UPDATE "+f.table("events")+" SET value=100 WHERE occurred_at='2026-01-01'")
	refresh()
	if n := f.integer(t, "SELECT sum(total) FROM "+f.table("daily")); n != 154 {
		t.Fatalf("aggregate did not refresh updates: %d", n)
	}
	f.exec(t, "ALTER TABLE "+f.table("events")+" SET(timescaledb.enable_columnstore=true,timescaledb.segmentby='tenant',timescaledb.orderby='occurred_at')")
	chunk := f.text(t, "SELECT show_chunks($1::regclass)::text LIMIT 1", f.table("events"))
	f.exec(t, "CALL convert_to_columnstore($1::regclass)", chunk)
	if n := f.integer(t, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema=$1 AND is_compressed", f.schema); n < 1 {
		t.Fatal("columnstore conversion missing")
	}
	if n := f.integer(t, "SELECT sum(value) FROM "+f.table("events")); n != 154 {
		t.Fatal("columnstore changed data")
	}
	job := f.integer(t, "SELECT add_retention_policy($1::regclass,drop_after:=interval '1 day',initial_start:=clock_timestamp()+interval '1 hour')", f.table("events"))
	f.exec(t, "CALL run_job($1::int)", job)
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("events")); n != 0 {
		t.Fatal("retention did not remove expired chunks")
	}
	f.exec(t, "SELECT remove_retention_policy($1::regclass)", f.table("events"))
}

func testDistributedTables(t *testing.T, db *sql.DB, c *Client) {
	requireCapability(t, db, "citus")
	f := ownCapability(t, db)
	workerDSN := os.Getenv("SERVEX_IT_CITUS_WORKER_DSN")
	if workerDSN == "" {
		t.Fatal("separate owned Citus worker DSN is required")
	}
	worker, err := sql.Open("pgx", workerDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	f.exec(t, "CREATE TABLE "+f.table("events")+" (tenant text NOT NULL,id int NOT NULL,value int,PRIMARY KEY(tenant,id))")
	if err := c.DistributeTable(f.ctx, Table{Schema: f.schema, Name: "events"}, "tenant"); err != nil {
		t.Fatal(err)
	}
	f.exec(t, "INSERT INTO "+f.table("events")+" VALUES('a',1,10),('a',2,20),('b',1,5)")
	if n := f.integer(t, "SELECT sum(value) FROM "+f.table("events")); n != 35 {
		t.Fatalf("distributed aggregate %d", n)
	}
	if n := f.integer(t, "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid=$1::regclass", f.table("events")); n < 2 {
		t.Fatal("table did not create multiple shards")
	}
	conn, err := worker.Conn(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(f.ctx, "SET citus.override_table_visibility=off"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := conn.QueryRowContext(f.ctx, "SELECT count(*) FROM pg_tables WHERE schemaname=$1 AND tablename LIKE 'events_%'", f.schema).Scan(&count); err != nil || count < 1 {
		t.Fatalf("no physical worker shards: %d %v", count, err)
	}
	tx, err := f.conn.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(f.ctx, "INSERT INTO "+f.table("events")+" VALUES('a',3,100),('b',3,200)"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("events")+" WHERE id=3"); n != 0 {
		t.Fatal("distributed rollback failed")
	}
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("events")+" WHERE id=1"); n != 2 {
		t.Fatal("tenant-key identity was lost")
	}
}

func testDuckDB(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "pg_duckdb")
	// pg_duckdb 1.1.1 can change aggregate result types after extended-protocol
	// Describe. Use its own simple-protocol pool so pgx decodes actual result types.
	cfg, err := pgx.ParseConfig(os.Getenv("SERVEX_IT_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	analytical := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = analytical.Close() })
	f := ownCapability(t, analytical)
	f.exec(t, "CREATE TABLE "+f.table("events")+" AS SELECT x AS id,x AS amount FROM generate_series(1,100) x")
	f.exec(t, "SET duckdb.force_execution=true")
	if n := f.integer(t, "SELECT sum(amount) FROM "+f.table("events")); n != 5050 {
		t.Fatal("analytical aggregate differs")
	}
	if plan := f.plan(t, "SELECT sum(amount) FROM "+f.table("events")); !strings.Contains(plan, "DuckDB") {
		t.Fatalf("analytical engine not selected: %s", plan)
	}
	file := "/tmp/" + f.schema + ".parquet"
	f.exec(t, "COPY (SELECT * FROM "+f.table("events")+") TO '"+file+"' (FORMAT parquet)")
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := f.conn.ExecContext(cleanup, "SET duckdb.force_execution=false"); err != nil {
			t.Error(err)
		}
		if out, err := exec.CommandContext(cleanup, "docker", "exec", ownedContainer(t), "rm", "-f", file).CombinedOutput(); err != nil {
			t.Errorf("remove owned Parquet fixture: %v: %s", err, out)
		}
	})
	if n := f.integer(t, "SELECT count(*) FROM read_parquet($1)", file); n != 100 {
		t.Fatalf("external Parquet row count %d", n)
	}
	if n := f.integer(t, "SELECT sum((r['amount'])::int) FROM read_parquet($1) r", file); n != 5050 {
		t.Fatalf("Parquet aggregate %d", n)
	}
	if n := f.integer(t, "SELECT sum(e.amount) FROM "+f.table("events")+" e JOIN read_parquet($1) r ON e.id=(r['id'])::int", file); n != 5050 {
		t.Fatalf("native/external join %d", n)
	}
}
