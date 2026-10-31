# PostgreSQL vector store (v1.0.0)

Implements servex `storage/vector.Store` using a caller-owned `*sql.DB`.
Document/query embeddings are supplied by the application. Construction has no
I/O, migrations or goroutines. Select a trusted namespace and a fixed dimension
(1..2000); supported metrics are cosine (default) and L2. Cosine rejects zero
vectors. All metadata labels match exactly with AND.

```go
store, err := pgvector.New(db, pgvector.Config{
    Namespace: tenantID, Dimensions: 1536,
    Table: postgres.Table{Schema: "public", Name: "document_vectors"},
})
if err != nil { return err }
if err := store.Migrate(ctx); err != nil { return err } // Explicit setup.
if err := store.Upsert(ctx, vector.Record{
    ID: "document-1", Vector: embedding,
    Metadata: map[string]string{"kind": "document"}, Payload: documentJSON,
}); err != nil { return err }
hits, err := store.Search(ctx, vector.Query{
    Vector: queryEmbedding, Metadata: map[string]string{"kind": "document"},
    Limit: 10, // Exact unless Approximate is explicitly true.
})
```

Imports: `servex/storage/vector`, `servex-contrib/vector/pgvector` and
`servex-contrib/database/postgres`. Enable the server's `vector` extension
separately. Migrate rejects a preexisting incompatible vector dimension.

Upsert batches are atomic. UpsertTx joins a caller's SQL transaction without
committing it. Delete is namespace-scoped and atomic. Payloads are opaque bytes.
Search returns vectors, labels, payloads and distance (smaller is closer). Results
are bounded to 10000. Namespace selection is not a database ACL.

CreateIndex is explicit, uses CREATE INDEX CONCURRENTLY, and must run outside a
transaction. Supported kinds: HNSW, IVFFlat (positive Lists), DiskANN
(`vectorscale` installed), VectorChord (`vchord` installed). Install compatible
server versions yourself. Exact queries materialize all eligible rows before
ordering so an ANN index cannot silently weaken recall. Approximate queries
permit the configured index and may return fewer filtered results. An index is
shared by the table's namespaces; plan tenant-specific partitions/indexes when
needed. Index method, filter selectivity, dimensions and memory limits require
workload-specific tuning; index creation does not promise a particular plan.

See [PostgreSQL integration](../../POSTGRESQL.md) for source-mode checks, pinned
fixtures and the capability/acceptance matrix. This source needs the matching
unreleased servex contracts and database/postgres module; no publication is implied.

The general fixture's real DiskANN and VectorChord acceptance runs cosine and
L2 separately. Each test builds its sole ANN index on a 512-record, 16-dimension
table, checks EXPLAIN selects that index, then verifies exact/approximate reads,
metadata/namespace isolation, updates, deletion and transaction rollback.
