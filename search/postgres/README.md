# PostgreSQL document search (v1.0.0)

Implements servex `storage/search.Index` using four explicitly selected engines:

| Engine | Requirement | Query semantics |
|---|---|---|
| Native | Built-in PostgreSQL | simple configuration, plainto_tsquery |
| ParadeDB | pg_search >= 0.20 | conjunction (&&&), BM25 via pdb.score |
| PGroonga | pgroonga >= 2.0.4 | Groonga query syntax (&@~) |
| Trigram | pg_trgm | positive string similarity, GiST distance ordering |

```go
index, err := pgsearch.New(db, pgsearch.Config{
    Namespace: tenantID, Engine: pgsearch.ParadeDB,
    Table: postgres.Table{Schema: "public", Name: "documents"},
})
if err != nil { return err }
if err := index.Migrate(ctx); err != nil { return err }
if err := index.CreateIndex(ctx, "documents_search_idx"); err != nil { return err }
if err := index.Upsert(ctx, search.Document{
    ID: "one", Text: "PostgreSQL search", Payload: []byte("opaque application data"),
}); err != nil { return err }
hits, err := index.Search(ctx, search.Query{Text: "PostgreSQL", Limit: 10})
```

Use the import alias `pgsearch "github.com/nostosx/servex-contrib/search/postgres"`
and contracts from `servex/storage/search`. Table creation/index construction
are explicit administrative steps. Constructors do no I/O and never own the pool.

The generated surrogate row_id is globally unique in the table, as required by
ParadeDB's key_field. Application IDs can repeat across trusted namespaces.
Both indexing and querying include the namespace; metadata uses exact string
labels. Upsert/Delete batches are atomic; UpsertTx joins a caller transaction.
Search rejects invalid text and bounds Limit to 10000 and Offset to 1000000.

Scores are engine/query-specific. PGroonga can return score zero when PostgreSQL
chooses a sequential scan; inspect EXPLAIN and indexes rather than inventing a
relevance score. Trigram's positive-similarity predicate does not depend on a
session-global threshold. Native tokenization is deliberately simple; language
analysis belongs to a selected extension/configuration. This adapter does not
hide engine differences, expose SQL fragments to query input, or supply a
universal highlight/facet API.

See [PostgreSQL integration](../../POSTGRESQL.md) for development and real-server
acceptance. New contracts and database/postgres are unreleased dependencies.
