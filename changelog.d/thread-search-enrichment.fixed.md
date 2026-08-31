`POST /threads/search` no longer costs two metadata round trips and one serial
graph-state read per result. Rows returned by the store are their own existence
proof, so the per-row `ensure_exists` re-read is gone, and state enrichment now
runs concurrently under a bound rather than one row after another. `app_threads`
also gains an index on `updated_at` — the default search sort.
