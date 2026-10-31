// Package pgvector implements generic vector storage using PostgreSQL's vector
// extension. It borrows a database/sql pool and never generates embeddings.
package pgvector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/nostosx/servex-contrib/database/postgres"
	"github.com/nostosx/servex/storage/vector"
)

type Metric string

const (
	Cosine Metric = "cosine"
	L2     Metric = "l2"
)

// Config fixes the dimension, metric and trusted namespace for one store.
// Namespace is logical isolation; the application must select it from authenticated identity.
type Config struct {
	Table      postgres.Table
	Namespace  string
	Dimensions int
	Metric     Metric
}
type Store struct {
	db                *sql.DB
	cfg               Config
	table             string
	operator, opclass string
}

var _ vector.Store = (*Store)(nil)

func New(db *sql.DB, cfg Config) (*Store, error) {
	if db == nil || !validText(cfg.Namespace) || len(cfg.Namespace) > 256 || cfg.Dimensions < 1 || cfg.Dimensions > 2000 {
		return nil, postgres.Invalid("database, namespace and dimensions (1..2000) are required")
	}
	if cfg.Table.Schema == "" {
		cfg.Table.Schema = "public"
	}
	if cfg.Table.Name == "" {
		cfg.Table.Name = "servex_vectors"
	}
	table, err := cfg.Table.SQL()
	if err != nil {
		return nil, err
	}
	if cfg.Metric == "" {
		cfg.Metric = Cosine
	}
	s := &Store{db: db, cfg: cfg, table: table}
	switch cfg.Metric {
	case Cosine:
		s.operator = "<=>"
		s.opclass = "vector_cosine_ops"
	case L2:
		s.operator = "<->"
		s.opclass = "vector_l2_ops"
	default:
		return nil, postgres.Invalid("unsupported distance metric")
	}
	return s, nil
}

func validText(s string) bool { return s != "" && utf8.ValidString(s) && !strings.ContainsRune(s, 0) }

func (s *Store) encode(v []float32) (string, error) {
	if len(v) != s.cfg.Dimensions {
		return "", postgres.Invalid("vector dimension mismatch")
	}
	var norm float64
	parts := make([]string, len(v))
	for i, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return "", postgres.Invalid("nonfinite vector")
		}
		norm += float64(x) * float64(x)
		parts[i] = strconv.FormatFloat(float64(x), 'g', -1, 32)
	}
	if norm == 0 && s.cfg.Metric == Cosine {
		return "", postgres.Invalid("cosine vectors must have nonzero norm")
	}
	return "[" + strings.Join(parts, ",") + "]", nil
}

// Migrate creates only this adapter's table. Extension installation is separate.
// An incompatible preexisting dimension is rejected rather than silently reused.
func (s *Store) Migrate(ctx context.Context) error {
	if err := postgres.Require(ctx, s.db, "vector"); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+s.table+` (namespace text COLLATE "C" NOT NULL,id text COLLATE "C" NOT NULL,embedding vector(`+strconv.Itoa(s.cfg.Dimensions)+`) NOT NULL,metadata jsonb NOT NULL,payload bytea NOT NULL,PRIMARY KEY(namespace,id))`)
	if err != nil {
		return postgres.Error(err)
	}
	var dimensions int
	err = s.db.QueryRowContext(ctx, `SELECT atttypmod FROM pg_attribute WHERE attrelid=$1::regclass AND attname='embedding' AND NOT attisdropped`, s.table).Scan(&dimensions)
	if err != nil {
		return postgres.Error(err)
	}
	if dimensions != s.cfg.Dimensions {
		return postgres.Invalid("existing vector table has a different dimension")
	}
	return nil
}

type encodedRecord struct {
	id, embedding, metadata string
	payload                 []byte
}

func (s *Store) prepare(records []vector.Record) ([]encodedRecord, error) {
	out := make([]encodedRecord, len(records))
	seen := map[string]bool{}
	for i, r := range records {
		if !validText(r.ID) || len(r.ID) > 1024 || seen[r.ID] {
			return nil, postgres.Invalid("record IDs must be valid and unique within a batch")
		}
		seen[r.ID] = true
		v, err := s.encode(r.Vector)
		if err != nil {
			return nil, err
		}
		m, err := postgres.Labels(r.Metadata)
		if err != nil {
			return nil, err
		}
		payload := r.Payload
		if payload == nil {
			payload = []byte{}
		}
		out[i] = encodedRecord{r.ID, v, m, payload}
	}
	return out, nil
}

func (s *Store) Upsert(ctx context.Context, records ...vector.Record) error {
	prepared, err := s.prepare(records)
	if err != nil {
		return err
	}
	if len(prepared) == 0 {
		return ctx.Err()
	}
	return postgres.Transaction(ctx, s.db, func(tx *sql.Tx) error { return s.upsert(ctx, tx, prepared) })
}

// UpsertTx joins a caller-owned transaction; it never commits or rolls it back.
func (s *Store) UpsertTx(ctx context.Context, tx *sql.Tx, records ...vector.Record) error {
	if tx == nil {
		return postgres.Invalid("nil transaction")
	}
	prepared, err := s.prepare(records)
	if err != nil {
		return err
	}
	return s.upsert(ctx, tx, prepared)
}
func (s *Store) upsert(ctx context.Context, tx *sql.Tx, records []encodedRecord) error {
	for _, r := range records {
		_, err := tx.ExecContext(ctx, `INSERT INTO `+s.table+` (namespace,id,embedding,metadata,payload) VALUES($1,$2,$3::vector,$4::jsonb,$5) ON CONFLICT(namespace,id) DO UPDATE SET embedding=EXCLUDED.embedding,metadata=EXCLUDED.metadata,payload=EXCLUDED.payload`, s.cfg.Namespace, r.id, r.embedding, r.metadata, r.payload)
		if err != nil {
			return postgres.Error(err)
		}
	}
	return ctx.Err()
}

func (s *Store) Delete(ctx context.Context, ids ...string) error {
	for _, id := range ids {
		if !validText(id) {
			return postgres.Invalid("invalid record ID")
		}
	}
	if len(ids) == 0 {
		return ctx.Err()
	}
	return postgres.Transaction(ctx, s.db, func(tx *sql.Tx) error {
		for _, id := range ids {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+s.table+` WHERE namespace=$1 AND id=$2`, s.cfg.Namespace, id); err != nil {
				return postgres.Error(err)
			}
		}
		return nil
	})
}

func (s *Store) Search(ctx context.Context, q vector.Query) ([]vector.Hit, error) {
	if q.Limit < 1 || q.Limit > 10000 {
		return nil, postgres.Invalid("search limit must be 1..10000")
	}
	v, err := s.encode(q.Vector)
	if err != nil {
		return nil, err
	}
	m, err := postgres.Labels(q.Metadata)
	if err != nil {
		return nil, err
	}
	columns := `id,embedding::text,metadata,payload,embedding ` + s.operator + ` $2::vector AS distance`
	base := `SELECT ` + columns + ` FROM ` + s.table + ` WHERE namespace=$1 AND metadata @> $3::jsonb`
	query := `WITH candidates AS MATERIALIZED (` + base + `) SELECT * FROM candidates ORDER BY distance,id LIMIT $4`
	if q.Approximate {
		query = base + ` ORDER BY embedding ` + s.operator + ` $2::vector LIMIT $4`
	}
	rows, err := s.db.QueryContext(ctx, query, s.cfg.Namespace, v, m, q.Limit)
	if err != nil {
		return nil, postgres.Error(err)
	}
	defer rows.Close()
	var hits []vector.Hit
	for rows.Next() {
		var h vector.Hit
		var embedding string
		var metadata []byte
		if err := rows.Scan(&h.Record.ID, &embedding, &metadata, &h.Record.Payload, &h.Distance); err != nil {
			return nil, postgres.Error(err)
		}
		if err := json.Unmarshal([]byte(embedding), &h.Record.Vector); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(metadata, &h.Record.Metadata); err != nil {
			return nil, err
		}
		hits = append(hits, h)
	}
	return hits, postgres.Error(rows.Err())
}

type IndexKind string

const (
	HNSW        IndexKind = "hnsw"
	IVFFlat     IndexKind = "ivfflat"
	DiskANN     IndexKind = "diskann"
	VectorChord IndexKind = "vchordrq"
)

// CreateIndex is an explicit administrative operation. It never changes search
// semantics: a query must still opt in to Approximate. Duplicate names are errors.
// Concurrent index builds must run outside a transaction. Lists applies only to IVFFlat.
func (s *Store) CreateIndex(ctx context.Context, name string, kind IndexKind, lists int) error {
	quoted, err := postgres.Ident(name)
	if err != nil {
		return err
	}
	options := ""
	switch kind {
	case HNSW:
	case IVFFlat:
		if lists < 1 || lists > 32768 {
			return postgres.Invalid("IVFFlat lists must be 1..32768")
		}
		options = " WITH (lists=" + strconv.Itoa(lists) + ")"
	case DiskANN:
		if err := postgres.Require(ctx, s.db, "vectorscale"); err != nil {
			return err
		}
	case VectorChord:
		if err := postgres.Require(ctx, s.db, "vchord"); err != nil {
			return err
		}
	default:
		return postgres.Invalid("unsupported vector index")
	}
	if kind != IVFFlat && lists != 0 {
		return postgres.Invalid("lists applies only to IVFFlat")
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("CREATE INDEX CONCURRENTLY %s ON %s USING %s (embedding %s)%s", quoted, s.table, kind, s.opclass, options))
	return postgres.Error(err)
}
