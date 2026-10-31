//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func testJSONSchema(t *testing.T, db *sql.DB, c *Client) {
	requireCapability(t, db, "pg_jsonschema")
	f := ownCapability(t, db)
	schema := []byte(`{"type":"object","required":["name"],"properties":{"name":{"type":"string"}},"additionalProperties":false}`)
	for _, item := range []struct {
		value string
		valid bool
	}{{`{"name":"Ada"}`, true}, {`{"name":42}`, false}, {`{}`, false}, {`{"name":"Ada","other":true}`, false}} {
		valid, err := c.ValidateJSON(f.ctx, schema, []byte(item.value))
		if err != nil || valid != item.valid {
			t.Fatalf("validation %s: %v %v", item.value, valid, err)
		}
	}
	f.exec(t, "CREATE TABLE "+f.table("docs")+` (body jsonb CHECK(jsonb_matches_schema('{"type":"object","required":["name"]}'::json,body)))`)
	f.exec(t, "INSERT INTO "+f.table("docs")+" VALUES($1::jsonb)", `{"name":"Ada"}`)
	if _, err := f.conn.ExecContext(f.ctx, "INSERT INTO "+f.table("docs")+" VALUES('{}')"); err == nil {
		t.Fatal("invalid document bypassed CHECK")
	}
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("docs")); n != 1 {
		t.Fatalf("rows %d", n)
	}
}

func testIncrementalView(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "pg_ivm")
	f := ownCapability(t, db)
	f.exec(t, "CREATE TABLE "+f.table("events")+" (id int PRIMARY KEY,tenant text,amount int)")
	f.exec(t, "INSERT INTO "+f.table("events")+" VALUES(1,'a',10),(2,'a',20),(3,'b',5)")
	f.exec(t, "SELECT pgivm.create_immv($1,$2)", f.schema+".summary", "SELECT tenant,sum(amount) AS total,count(*) AS n FROM "+f.table("events")+" GROUP BY tenant")
	assert := func(want int64) {
		t.Helper()
		if n := f.integer(t, "SELECT total FROM "+f.table("summary")+" WHERE tenant='a'"); n != want {
			t.Fatalf("incremental total %d, want %d", n, want)
		}
	}
	assert(30)
	f.exec(t, "UPDATE "+f.table("events")+" SET amount=50 WHERE id=1")
	assert(70)
	tx, err := f.conn.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(f.ctx, "INSERT INTO "+f.table("events")+" VALUES(4,'a',5)"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	var total int64
	if err := tx.QueryRowContext(f.ctx, "SELECT total FROM "+f.table("summary")+" WHERE tenant='a'").Scan(&total); err != nil || total != 75 {
		_ = tx.Rollback()
		t.Fatalf("transactional aggregate %d: %v", total, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assert(70)
	f.exec(t, "DELETE FROM "+f.table("events")+" WHERE id=2")
	assert(50)
}

func testGraph(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "age")
	f := ownCapability(t, db)
	graph := f.schema + "_graph"
	f.exec(t, "LOAD 'age'")
	f.exec(t, "SET search_path=ag_catalog,public")
	f.exec(t, "SELECT ag_catalog.create_graph($1::name)", graph)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := f.conn.ExecContext(cleanup, "SELECT ag_catalog.drop_graph($1::name,true)", graph); err != nil {
			t.Error(err)
		}
	})
	query := `SELECT value::text FROM ag_catalog.cypher('` + graph + `',$$ CREATE (a:Person {name:'Ada'})-[:KNOWS]->(b:Person {name:'Bob'}) RETURN a.name $$) AS (value ag_catalog.agtype)`
	if got := f.text(t, query); got != "Ada" {
		t.Fatalf("created name %s", got)
	}
	query = `SELECT value::text FROM ag_catalog.cypher('` + graph + `',$$ MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*) $$) AS (value ag_catalog.agtype)`
	if got := f.text(t, query); got != "1" {
		t.Fatalf("edge count %s", got)
	}
	statement := "probe_" + f.schema
	f.exec(t, `PREPARE `+statement+`(ag_catalog.agtype) AS SELECT value::text FROM ag_catalog.cypher('`+graph+`',$$ MATCH (n:Person) WHERE n.name=$name RETURN n.name $$,$1) AS (value ag_catalog.agtype)`)
	if got := f.text(t, `EXECUTE `+statement+`('{"name":"Ada"}'::ag_catalog.agtype)`); got != "Ada" {
		t.Fatalf("parameterized Cypher %s", got)
	}
	f.exec(t, "DEALLOCATE "+statement)
	tx, err := f.conn.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(f.ctx, `SELECT * FROM ag_catalog.cypher('`+graph+`',$$ CREATE (:Person {name:'rollback'}) $$) AS (value ag_catalog.agtype)`); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := f.text(t, `SELECT value::text FROM ag_catalog.cypher('`+graph+`',$$ MATCH (n:Person) RETURN count(n) $$) AS (value ag_catalog.agtype)`); got != "2" {
		t.Fatalf("graph rollback count %s", got)
	}
}

func testPartitions(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "pg_partman")
	f := ownCapability(t, db)
	parent := f.schema + ".events"
	f.exec(t, "CREATE TABLE "+f.table("events")+" (occurred_at timestamptz NOT NULL,id bigint) PARTITION BY RANGE(occurred_at)")
	start := time.Now().UTC().AddDate(0, 0, -3).Format("2006-01-02")
	f.exec(t, `SELECT partman.create_parent(p_parent_table:=$1,p_control:='occurred_at',p_interval:='1 day',p_premake:=1,p_start_partition:=$2)`, parent, start)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.ExecContext(cleanup, "DELETE FROM partman.part_config WHERE parent_table=$1", parent); err != nil {
			t.Error(err)
		}
	})
	f.exec(t, "INSERT INTO "+f.table("events")+" VALUES(current_date-interval '2 days',1),(current_timestamp,2),(current_date+interval '1 day',3)")
	expired := f.integer(t, "SELECT tableoid::oid FROM "+f.table("events")+" WHERE id=1")
	before := f.integer(t, "SELECT count(*) FROM pg_inherits WHERE inhparent=$1::regclass", parent)
	if before < 3 {
		t.Fatalf("partition creation count %d", before)
	}
	f.exec(t, "UPDATE partman.part_config SET retention='1 day',retention_keep_table=false WHERE parent_table=$1", parent)
	f.exec(t, "SELECT partman.run_maintenance(p_parent_table:=$1,p_analyze:=false)", parent)
	if n := f.integer(t, "SELECT count(*) FROM pg_class WHERE oid=$1::oid", expired); n != 0 {
		t.Fatal("expired partition was detached but not dropped")
	}
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("events")+" WHERE id=1"); n != 0 {
		t.Fatal("retention kept expired row")
	}
	if n := f.integer(t, "SELECT count(*) FROM "+f.table("events")+" WHERE id IN(2,3)"); n != 2 {
		t.Fatal("retention lost current/future data")
	}
	if after := f.integer(t, "SELECT count(*) FROM pg_inherits WHERE inhparent=$1::regclass", parent); after >= before {
		t.Fatalf("no partition was retired: %d -> %d", before, after)
	}
}

func testSpatial(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "postgis")
	f := ownCapability(t, db)
	f.exec(t, "CREATE TABLE "+f.table("places")+" (id int PRIMARY KEY,position geography(Point,4326))")
	f.exec(t, "INSERT INTO "+f.table("places")+" VALUES(1,ST_SetSRID(ST_MakePoint(103.85,1.29),4326)),(2,ST_SetSRID(ST_MakePoint(-0.12,51.5),4326))")
	f.exec(t, "CREATE INDEX places_position ON "+f.table("places")+" USING gist(position)")
	query := "SELECT id FROM " + f.table("places") + " WHERE ST_DWithin(position,ST_SetSRID(ST_MakePoint($1,$2),4326)::geography,$3)"
	if id := f.integer(t, query, 103.85, 1.29, 10000); id != 1 {
		t.Fatalf("nearby place %d", id)
	}
	if n := f.integer(t, "SELECT count(*) FROM ("+query+") AS nearby", 103.85, 1.29, 10000); n != 1 {
		t.Fatalf("spatial query included a distant place: %d", n)
	}
	f.exec(t, "SET enable_seqscan=off")
	plan := f.plan(t, "SELECT id FROM "+f.table("places")+" WHERE ST_DWithin(position,ST_SetSRID(ST_MakePoint(103.85,1.29),4326)::geography,10000)")
	if !strings.Contains(plan, "Index Scan") {
		t.Fatalf("spatial index not used: %s", plan)
	}
}

func testHypotheticalIndex(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "hypopg")
	f := ownCapability(t, db)
	f.exec(t, "CREATE TABLE "+f.table("events")+" AS SELECT x AS id,repeat('x',50) AS body FROM generate_series(1,10000) x")
	f.exec(t, "ANALYZE "+f.table("events"))
	f.exec(t, "SET enable_seqscan=off")
	if id := f.integer(t, "SELECT indexrelid FROM hypopg_create_index($1)", "CREATE INDEX ON "+f.table("events")+"(id)"); id <= 0 {
		t.Fatal("hypothetical index missing")
	}
	if plan := f.plan(t, "SELECT * FROM "+f.table("events")+" WHERE id=42"); !strings.Contains(plan, "Index Scan") {
		t.Fatalf("hypothetical index not planned: %s", plan)
	}
	if n := f.integer(t, "SELECT count(*) FROM pg_index WHERE indrelid=$1::regclass", f.table("events")); n != 0 {
		t.Fatal("hypothetical index became a physical index")
	}
	var other int64
	if err := db.QueryRowContext(f.ctx, "SELECT count(*) FROM hypopg_list_indexes").Scan(&other); err != nil || other != 0 {
		t.Fatalf("hypothetical state leaked to another connection: %d %v", other, err)
	}
	f.exec(t, "SELECT hypopg_reset()")
	if n := f.integer(t, "SELECT count(*) FROM hypopg_list_indexes"); n != 0 {
		t.Fatal("hypothetical reset failed")
	}
}

func testStatementStatistics(t *testing.T, db *sql.DB, _ *Client) {
	requireCapability(t, db, "pg_stat_statements")
	f := ownCapability(t, db)
	f.exec(t, "CREATE TABLE "+f.table("events")+" (id int)")
	for range 2 {
		_ = f.integer(t, "SELECT count(*) FROM "+f.table("events")+" WHERE id=$1", 42)
	}
	n := f.integer(t, `SELECT count(*) FROM pg_stat_statements WHERE query LIKE '%'||$1||'%' AND calls>=2`, f.schema)
	if n < 1 {
		t.Fatal("executed statement was not recorded")
	}
}
