`POST /threads/search` no longer costs an extra metadata round trip and a serial
graph-state read per result. Rows returned by the store are their own existence
proof, so the per-row `ensure_exists` re-read is gone — a page now costs the one
page-level metadata query rather than that query plus a lookup per row — and
state enrichment runs concurrently under a bound shared by every search, rather
than one row after another. `app_threads` also gains an index on `updated_at`,
the default search sort.
