// Package postgres implements document search with selectable PostgreSQL engines.
// The caller owns the SQL pool and explicitly creates tables and indexes.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"unicode/utf8"

	pg "github.com/nostosx/servex-contrib/database/postgres"
	"github.com/nostosx/servex/storage/search"
)

type Engine string

const (
	// Native uses PostgreSQL's simple text configuration and plainto_tsquery.
	Native Engine = "native"
	// ParadeDB uses pg_search >= 0.20's conjunction operator and pdb.score.
	ParadeDB Engine = "pg_search"
	// PGroonga uses Groonga query syntax (&@~). Its score is zero on sequential scans.
	PGroonga Engine = "pgroonga"
	// Trigram uses pg_trgm similarity, without a connection-global threshold.
	Trigram Engine = "pg_trgm"
)

type Config struct {
	Table     pg.Table
	Namespace string
	Engine    Engine
}
type Index struct {
	db    *sql.DB
	cfg   Config
	table string
}

var _ search.Index = (*Index)(nil)

func New(db *sql.DB, cfg Config) (*Index, error) {
	if db == nil || !text(cfg.Namespace) || len(cfg.Namespace) > 256 {
		return nil, pg.Invalid("database and trusted namespace are required")
	}
	if cfg.Table.Schema == "" {
		cfg.Table.Schema = "public"
	}
	if cfg.Table.Name == "" {
		cfg.Table.Name = "servex_documents"
	}
	if cfg.Engine == "" {
		cfg.Engine = Native
	}
	switch cfg.Engine {
	case Native, ParadeDB, PGroonga, Trigram:
	default:
		return nil, pg.Invalid("unsupported search engine")
	}
	t, err := cfg.Table.SQL()
	if err != nil {
		return nil, err
	}
	return &Index{db, cfg, t}, nil
}
func text(s string) bool { return s != "" && utf8.ValidString(s) && !strings.ContainsRune(s, 0) }

func (s *Index) Migrate(ctx context.Context) error {
	if s.cfg.Engine != Native {
		if err := pg.Require(ctx, s.db, string(s.cfg.Engine)); err != nil {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+s.table+` (row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,namespace text COLLATE "C" NOT NULL,id text COLLATE "C" NOT NULL,body text NOT NULL,metadata jsonb NOT NULL,payload bytea NOT NULL,UNIQUE(namespace,id))`)
	return pg.Error(err)
}

// CreateIndex is separate from Migrate. It rejects duplicate names and never
// silently replaces or changes the tokenizer of an existing index.
func (s *Index) CreateIndex(ctx context.Context, name string) error {
	name, err := pg.Ident(name)
	if err != nil {
		return err
	}
	var definition string
	switch s.cfg.Engine {
	case Native:
		definition = `USING gin (to_tsvector('simple',body))`
	case ParadeDB:
		definition = `USING bm25 (row_id,body,namespace) WITH (key_field='row_id')`
	case PGroonga:
		definition = `USING pgroonga (body)`
	case Trigram:
		definition = `USING gist (body gist_trgm_ops)`
	}
	_, err = s.db.ExecContext(ctx, `CREATE INDEX `+name+` ON `+s.table+` `+definition)
	return pg.Error(err)
}

type document struct {
	id, body, metadata string
	payload            []byte
}

func prepare(records []search.Document) ([]document, error) {
	result := make([]document, len(records))
	seen := map[string]bool{}
	for i, r := range records {
		if !text(r.ID) || len(r.ID) > 1024 || seen[r.ID] || !utf8.ValidString(r.Text) || strings.ContainsRune(r.Text, 0) {
			return nil, pg.Invalid("invalid or duplicate document")
		}
		seen[r.ID] = true
		m, err := pg.Labels(r.Metadata)
		if err != nil {
			return nil, err
		}
		p := r.Payload
		if p == nil {
			p = []byte{}
		}
		result[i] = document{r.ID, r.Text, m, p}
	}
	return result, nil
}
func (s *Index) Upsert(ctx context.Context, records ...search.Document) error {
	prepared, err := prepare(records)
	if err != nil {
		return err
	}
	if len(prepared) == 0 {
		return ctx.Err()
	}
	return pg.Transaction(ctx, s.db, func(tx *sql.Tx) error { return s.upsert(ctx, tx, prepared) })
}
func (s *Index) UpsertTx(ctx context.Context, tx *sql.Tx, records ...search.Document) error {
	if tx == nil {
		return pg.Invalid("nil transaction")
	}
	prepared, err := prepare(records)
	if err != nil {
		return err
	}
	return s.upsert(ctx, tx, prepared)
}
func (s *Index) upsert(ctx context.Context, tx *sql.Tx, records []document) error {
	for _, r := range records {
		_, err := tx.ExecContext(ctx, `INSERT INTO `+s.table+` (namespace,id,body,metadata,payload) VALUES($1,$2,$3,$4::jsonb,$5) ON CONFLICT(namespace,id) DO UPDATE SET body=EXCLUDED.body,metadata=EXCLUDED.metadata,payload=EXCLUDED.payload`, s.cfg.Namespace, r.id, r.body, r.metadata, r.payload)
		if err != nil {
			return pg.Error(err)
		}
	}
	return ctx.Err()
}
func (s *Index) Delete(ctx context.Context, ids ...string) error {
	for _, id := range ids {
		if !text(id) {
			return pg.Invalid("invalid document ID")
		}
	}
	if len(ids) == 0 {
		return ctx.Err()
	}
	return pg.Transaction(ctx, s.db, func(tx *sql.Tx) error {
		for _, id := range ids {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+s.table+` WHERE namespace=$1 AND id=$2`, s.cfg.Namespace, id); err != nil {
				return pg.Error(err)
			}
		}
		return nil
	})
}
func (s *Index) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	if q.Limit < 1 || q.Limit > 10000 || q.Offset < 0 || q.Offset > 1000000 || !text(q.Text) {
		return nil, pg.Invalid("invalid search text, limit or offset")
	}
	m, err := pg.Labels(q.Metadata)
	if err != nil {
		return nil, err
	}
	var score, predicate string
	switch s.cfg.Engine {
	case Native:
		score = `ts_rank_cd(to_tsvector('simple',body),plainto_tsquery('simple',$2))`
		predicate = `to_tsvector('simple',body) @@ plainto_tsquery('simple',$2)`
	case ParadeDB:
		score = `pdb.score(row_id)`
		predicate = `body &&& $2::text`
	case PGroonga:
		score = `pgroonga_score(tableoid,ctid)`
		predicate = `body &@~ $2::text`
	case Trigram:
		score = `similarity(body,$2)`
		predicate = `similarity(body,$2)>0`
	}
	order := "score DESC,id"
	if s.cfg.Engine == Trigram {
		order = "body <-> $2,id"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,body,metadata,payload,`+score+` AS score FROM `+s.table+` WHERE namespace=$1 AND `+predicate+` AND metadata @> $3::jsonb ORDER BY `+order+` LIMIT $4 OFFSET $5`, s.cfg.Namespace, q.Text, m, q.Limit, q.Offset)
	if err != nil {
		return nil, pg.Error(err)
	}
	defer rows.Close()
	var hits []search.Hit
	for rows.Next() {
		var h search.Hit
		var m []byte
		if err := rows.Scan(&h.Document.ID, &h.Document.Text, &m, &h.Document.Payload, &h.Score); err != nil {
			return nil, pg.Error(err)
		}
		if err := json.Unmarshal(m, &h.Document.Metadata); err != nil {
			return nil, err
		}
		hits = append(hits, h)
	}
	return hits, pg.Error(rows.Err())
}
