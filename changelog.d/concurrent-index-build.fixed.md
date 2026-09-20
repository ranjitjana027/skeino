Metadata-store indexes are now built with `CREATE INDEX CONCURRENTLY` on their
own autocommit connection instead of inside `setup()`'s transaction. A plain
`CREATE INDEX` holds a lock that blocks writes for the whole table scan, so
starting an instance against an existing large `app_threads` or `app_runs` could
stall traffic until the build finished. An index left invalid by an interrupted
build is dropped and rebuilt rather than skipped forever by `IF NOT EXISTS`.
Index maintenance runs under a Postgres advisory lock, so replicas starting
together cannot mistake a peer's in-progress build for an invalid leftover and
drop it; an instance that does not get the lock skips the work its peer is
already doing rather than blocking startup behind it.
