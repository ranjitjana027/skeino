# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Unreleased changes live as [changelog fragments](https://github.com/ranjitjana027/skeino/blob/main/changelog.d/README.md)
under `changelog.d/` and are collated here on release with `towncrier build`.

<!-- towncrier release notes start -->

## [3.1.0] - 2026-09-22

### Added

- LangGraph Studio can now show traces for runs on a skeino server. Studio only
  does so when `GET /info` advertises session-name tracing, and skeino reported
  nothing but `{status, name, version}` — so Studio refused ("Studio tracing
  requires langgraph-api 0.11.0 or later with session-name tracing enabled").
  `/info` now also returns langgraph-api's `langgraph_py_version`, `flags`
  (`langsmith_tracing_session_on_runs`, `langsmith_tracing_replicas`, a live
  `langsmith` flag, `crons: false`) and `host`. Runs accept
  `langsmith_tracer: {project_name, example_id}` and, when tracing is enabled,
  execute inside a LangSmith tracing context that writes the trace to that project
  as well as the server's default one; each run reports the project as
  `langsmith_session_name`. With tracing off, the field is accepted and ignored. ([#106](https://github.com/ranjitjana027/skeino/issues/106))


## [3.0.1] - 2026-09-22

### Fixed

- Interrupts now reach clients as data instead of as text. LangGraph's `Interrupt`
  is a slotted dataclass, which the outbound serializer could not introspect, so
  it fell back to `str()` and streamed the Python repr — an approval UI reading
  `interrupt.value` found a string. Dataclasses are now serialized from their
  declared fields, in both the streaming and state-snapshot serializers. ([#104](https://github.com/ranjitjana027/skeino/issues/104))
- A thread parked on an `interrupt()` now reports `status: "interrupted"` instead
  of `"idle"`, matching LangGraph Platform. The status was written from the run's
  outcome alone, and a run that ends waiting for a human decision ends
  successfully — so a thread waiting on an approval was indistinguishable from one
  with nothing pending. The status is now read from the checkpoint after the run
  settles; an unreadable checkpoint still falls back to `"idle"`. ([#104](https://github.com/ranjitjana027/skeino/issues/104))
- A paused run now reaches the client. The output-schema filter treated
  LangGraph's reserved `__interrupt__` channel as ordinary graph state and
  stripped it from `values` events, so a graph that called `interrupt()` looked to
  the SDK like a run that simply stopped — no approval prompt, no way to resume.
  Reserved dunder channels are now exempt from the filter in both `values` and
  `updates` events; graph state is still filtered exactly as before. ([#104](https://github.com/ranjitjana027/skeino/issues/104))


## [3.0.0] - 2026-09-20

### Added

- Background (async) runs. `POST /threads/{id}/runs` now starts the graph in a background task and returns immediately with a `pending`/`running` run. New endpoints to consume them: `POST /threads/{id}/runs/wait` (run to completion, returns the final state values), `GET /threads/{id}/runs/{rid}/join` (wait for a terminal state), `POST /threads/{id}/runs/{rid}/cancel` (`action=interrupt|rollback`, optional `wait`), and `DELETE /threads/{id}/runs/{rid}`. The `rollback` and `interrupt` multitask strategies are now implemented (cancel — and, for rollback, delete — the active run) instead of all returning 409 like `reject`. ([#18](https://github.com/ranjitjana027/skeino/issues/18))

### Changed

- **Breaking:** `POST /threads/{id}/runs` is now a background create that returns immediately with a non-terminal `RunModel`, instead of blocking until the graph finished. To run synchronously and get the output back, call the new `POST /threads/{id}/runs/wait` (which returns the final graph state values and the `X-Tokens-Used` header). ([#18](https://github.com/ranjitjana027/skeino/issues/18))

### Fixed

- Metadata-store indexes are now built with `CREATE INDEX CONCURRENTLY` on their
  own autocommit connection instead of inside `setup()`'s transaction. A plain
  `CREATE INDEX` holds a lock that blocks writes for the whole table scan, so
  starting an instance against an existing large `app_threads` or `app_runs` could
  stall traffic until the build finished. An index left invalid by an interrupted
  build is dropped and rebuilt rather than skipped forever by `IF NOT EXISTS`.
  Index maintenance runs under a Postgres advisory lock, so replicas starting
  together cannot mistake a peer's in-progress build for an invalid leftover and
  drop it; an instance that does not get the lock skips the work its peer is
  already doing rather than blocking startup behind it. ([#concurrent-index-build](https://github.com/ranjitjana027/skeino/issues/concurrent-index-build))
- Pooled metadata-store connections are no longer handed out inside an open
  transaction. The pool's liveness probe ran `SELECT 1` on a connection that is
  not in autocommit, which starts a transaction, so every checkout could arrive
  `INTRANS`. Both the metadata store and the checkpointer now use
  `AsyncConnectionPool.check_connection`, which toggles autocommit around the
  probe so the connection comes back clean. A failure of the metadata-store index
  advisory unlock also no longer masks the index-maintenance error that caused it. ([#pool-check-connection](https://github.com/ranjitjana027/skeino/issues/pool-check-connection))
- The Postgres metadata store now runs over a shared `AsyncConnectionPool` instead
  of opening a fresh connection for every operation. A connect + TLS handshake +
  SCRAM exchange per query dominated request latency against a managed Postgres in
  another region, and the cost scaled with the number of queries a request made.
  Connections are validated before checkout and prepared statements are disabled,
  so the store also stays correct behind a transaction-mode pooler. ([#pool-metadata-store](https://github.com/ranjitjana027/skeino/issues/pool-metadata-store))
- The Postgres checkpointer and metadata store now genuinely disable client-side
  prepared statements behind a transaction-mode pooler. Both pools passed
  `prepare_threshold=0`, which psycopg reads as *prepare on the first execution* —
  the opposite of the intent — so a query could still be prepared and then fail
  with `prepared statement "_pg3_0" does not exist` once pgbouncer or Supabase
  routed a later call to a different server-side session. The value is now `None`,
  which is what actually turns preparation off. ([#prepare-threshold-disable](https://github.com/ranjitjana027/skeino/issues/prepare-threshold-disable))
- `ThreadOps` now rejects a `search_enrich_concurrency` below 1 instead of
  accepting it and hanging. `asyncio.Semaphore` rejects negative values but
  accepts `0`, and a bound of `0` is never acquirable — every thread search would
  have waited on it forever, raising nothing and logging nothing. Construction
  now fails loudly with a `ValueError`. ([#search-bound-validation](https://github.com/ranjitjana027/skeino/issues/search-bound-validation))
- `POST /threads/search` no longer costs an extra metadata round trip and a serial
  graph-state read per result. Rows returned by the store are their own existence
  proof, so the per-row `ensure_exists` re-read is gone — a page now costs the one
  page-level metadata query rather than that query plus a lookup per row — and
  state enrichment runs concurrently under a bound shared by every search, rather
  than one row after another. `app_threads` also gains an index on `updated_at`,
  the default search sort. ([#thread-search-enrichment](https://github.com/ranjitjana027/skeino/issues/thread-search-enrichment))
- Redis (and SQLite) checkpoints now carry their `run_id`, so checkpoint→run
  grouping (e.g. in LangGraph Studio) works on every durable backend, not just
  Postgres. The run-enriching checkpointer wrapper was Postgres-only — it
  subclassed `AsyncPostgresSaver` — so Redis and SQLite snapshots were written
  with no `run_id`. It is now a backend-agnostic delegating wrapper applied to the
  Postgres, SQLite, and Redis builders; MongoDB already merges `run_id` natively
  and is left unwrapped. ([#49](https://github.com/ranjitjana027/skeino/issues/49))


## [2.2.0] - 2026-09-02

### Added

- Stateless runs: `POST /runs`, `/runs/wait`, `/runs/stream`, and `/runs/batch`
  execute against a thread created and deleted inside the request, so a one-shot
  invocation needs no thread lifecycle from the caller. Previously every client
  that wanted one open-coded the same three steps — create a thread, run, delete
  it — and a client that skipped the cleanup leaked a thread per request. The
  ephemeral thread and its checkpoints are removed whether the run succeeds,
  fails, or the stream is abandoned mid-flight. `POST /runs` runs synchronously
  rather than in the background (skeino has no background executor yet), and a
  stateless run rejects `checkpoint` with a 400: there is no history to resume
  from. ([#21](https://github.com/ranjitjana027/skeino/issues/21))


## [2.1.1] - 2026-08-02

### Fixed

- Request bodies now appear in the generated OpenAPI schema (`/openapi.json`,
  `/docs`, and the Scalar API explorer). skeino's routers parse JSON bodies by
  hand to tolerate `text/plain` payloads, which previously kept their request
  models (`RunCreateRequest`, `ThreadCreateRequest`, `ThreadSearchRequest`,
  `ThreadStateUpdateRequest`, `ThreadStateSearchRequest`, `AssistantSearchRequest`,
  `ThreadPatchRequest`, `CheckpointConfigModel`) — and their per-field
  descriptions — out of the documented schema. The tolerant parsing and the 422
  error contract are unchanged. ([#67](https://github.com/ranjitjana027/skeino/issues/67))


## [2.1.0] - 2026-06-26

### Changed

- Every request/response schema field now carries a description. These render in the Python API reference for all models; response models additionally surface them in the generated OpenAPI schema, Swagger UI (`/docs`), and the API explorer. ([#68](https://github.com/ranjitjana027/skeino/issues/68))

### Fixed

- Postgres checkpointer now runs over a liveness-checked `AsyncConnectionPool` instead of a single long-lived connection. A connection dropped by the server or a connection pooler (e.g. a Supabase/pgbouncer idle-timeout or recycle) is now detected and replaced on checkout, instead of wedging every subsequent checkpoint read with `OperationalError: the connection is closed`. Prepared statements are disabled (`prepare_threshold=0`) so the saver is also correct behind a transaction-mode pooler; pool size is configurable via the `pool_max_size` checkpointer option (default 10). ([#70](https://github.com/ranjitjana027/skeino/issues/70))


## [2.0.2] - 2026-06-20

### Security

- Bumped transitive dependency `langsmith` 0.8.5→0.8.18 to resolve a high-severity
  advisory (LangSmith SDK `TracingMiddleware` arbitrary server-side file read).


## [2.0.1] - 2026-06-20

### Changed

- Bumped dependencies: fastapi 0.136.3→0.138.0, starlette 1.1.0→1.3.1,
  langchain 1.3.4→1.3.9, langchain-core 1.4.7→1.4.8, langgraph 1.2.4→1.2.6,
  pydantic-settings 2.14.1→2.14.2, and dev tooling (pytest 9.0.3→9.1.1,
  ruff 0.15.17→0.15.18). CI now uses actions/checkout v7.


## [2.0.0] - 2026-06-14

### Changed

- Streaming now mirrors a real LangGraph server: each requested `stream_mode` is forwarded faithfully (`values` = full state per super-step, `updates` = per-node deltas, `custom` = graph stream-writer events) instead of synthesising full-history `values` snapshots from the message stream. `updates` events are now passed through the same fail-closed output-key filter as `values`, so internal pipeline fields never leak in node deltas. ([#57](https://github.com/ranjitjana027/skeino/issues/57))

### Removed

- `SkeinoSettings.agent_nodes` and `SkeinoSettings.status_field`, along with the non-standard token-accumulation streaming path they fed. Consumers that want live progress should emit it from the graph via LangGraph's `get_stream_writer()` (`custom` stream mode); clients select incremental streaming with standard modes such as `updates`. ([#57](https://github.com/ranjitjana027/skeino/issues/57))


## [1.1.0] - 2026-06-10

### Changed

- The SQLite metadata store enables `journal_mode=WAL` and a 10 s busy timeout
  at setup, preventing `database is locked` errors when sharing a database file
  with the SQLite checkpointer. WAL persists in the database file once enabled. ([#36](https://github.com/ranjitjana027/skeino/issues/36))
- Metadata store row shapes are now a typed contract: `ThreadRow`/`RunRow`
  TypedDicts (exported from `skeino.persistence`) replace the `dict[str, Any]`
  returns on `MetadataStoreProtocol`, and every backend now always includes the
  `error` key on run rows (previously the in-memory store omitted it until a
  failure and kept stale values across status updates). HTTP responses are
  unchanged; custom `MetadataStoreProtocol` implementations should return the
  new shapes. ([#36](https://github.com/ranjitjana027/skeino/issues/36))
- MongoDB: both the checkpointer and the metadata store now use the database
  named in the `mongodb://…/<db>` URI path, so graph state and metadata share
  the operator's chosen database. URIs without a path keep the previous
  defaults (`checkpointing_db` for checkpoints, `skeino` for metadata). If your
  URI already names a database, both stores re-point to it on upgrade —
  existing data in the old default databases is not migrated. ([#36](https://github.com/ranjitjana027/skeino/issues/36))

### Fixed

- PyPI trove classifier updated from `Development Status :: 4 - Beta` to `5 - Production/Stable` to match the stable 1.x release line. ([#48](https://github.com/ranjitjana027/skeino/issues/48))
- Per-run token usage is now measured with a `UsageMetadataCallbackHandler`
  attached to each run's config, so `X-Tokens-Used` and the streaming `end`
  event report the run's own tokens — including for graphs that never store
  usage-bearing messages in checkpoint state (previously reported as 0), and
  without the cumulative over-count on multi-turn threads. Summing the final
  checkpoint's messages remains as a fallback for providers the handler can't
  see. ([#52](https://github.com/ranjitjana027/skeino/issues/52))


## [1.0.1] - 2026-06-10

### Fixed

- Token-by-token `values` streaming now works for real langgraph-sdk clients. Two fixes: (1) the incremental accumulator engages when `values` is *among* the requested stream modes (SDK clients send `["values", "messages-tuple", "custom"]`), instead of only on an exact `== ["values"]` match that never fired — and it now forwards `custom` (UI) events so generative-UI consumers don't regress; (2) output-schema value filtering introspects **TypedDict** output schemas (the common `StateGraph(State, output=OutputState)` pattern) via `__annotations__` instead of failing closed and stripping every field — previously it dropped `messages` from every streamed event, so clients only saw the message after the post-run state fetch. Genuinely opaque schemas still fail closed. ([#42](https://github.com/ranjitjana027/skeino/pull/42))


## [1.0.0] - 2026-06-07

### Added

- Pluggable, optional database backends selected by `checkpointer_scheme`: **SQLite** (`skeino[sqlite]`), **PostgreSQL** (`skeino[postgres]`), and **MongoDB** (`skeino[mongodb]`) — each with a native durable metadata store (`SqliteMetadataStore`, `MetadataStore`, `MongoMetadataStore`) — plus a lazy `redis` checkpointer builder. All DB drivers are imported lazily, so the default install ships only the in-memory backend. ([#25](https://github.com/ranjitjana027/skeino/issues/25))

### Changed

- **Breaking:** persistence is now **scheme-authoritative**. `checkpointer_scheme` (default `"memory"`) alone selects the backend for *both* the checkpointer and the metadata store; the new `checkpointer_uri` is only the connection string for that scheme. A URI without a matching scheme is ignored (e.g. `checkpointer_scheme="memory"` with a Postgres URI still uses in-memory). `langgraph.json`'s `store.uri` now maps to `checkpointer_uri` with the scheme derived from the URI prefix. ([#25](https://github.com/ranjitjana027/skeino/issues/25))
- Adopted [towncrier](https://towncrier.readthedocs.io/) changelog fragments (`changelog.d/`): contributors now add a per-change fragment instead of editing `CHANGELOG.md`, so concurrent PRs no longer conflict on the changelog. ([#27](https://github.com/ranjitjana027/skeino/issues/27))

### Removed

- **Breaking:** removed the `postgres_uri` and `sqlite_path` settings (which doubled as backend selectors) in favour of `checkpointer_scheme` + `checkpointer_uri`. PostgreSQL is no longer a hard dependency — install `skeino[postgres]` for it. ([#25](https://github.com/ranjitjana027/skeino/issues/25))

### Fixed

- `skeino.__version__` is now derived from the installed package metadata instead of a hard-coded literal that drifted out of sync with `pyproject.toml` (it had been stuck at `0.1.0`). ([#35](https://github.com/ranjitjana027/skeino/issues/35))


## [0.3.0] - 2026-06-07

### Added

- Thread mutation & time-travel endpoints: `PATCH /threads/{id}` (update
  metadata), `DELETE /threads/{id}` (delete the thread, its runs, and its
  checkpoints), `POST /threads/{id}/state` (human-in-the-loop state edit,
  returning the new checkpoint), and reads at a specific checkpoint via
  `GET /threads/{id}/state/{checkpoint_id}` and `POST /threads/{id}/state/checkpoint`.
- `POST /threads/{thread_id}/copy` (and `ThreadOps.copy`) — fork a thread into an
  independent copy seeded with the source's latest state. Metadata is copied and
  stamped with `forked_from`. The copy is shallow (latest state only, not the
  full checkpoint history) and works across the in-memory and Postgres backends.

## [0.2.0] - 2026-06-07

### Changed

- The `status` filter on `GET /threads/{id}/runs` is now typed as the `RunStatus`
  literal, so invalid values are rejected at the API edge with a 422 instead of
  by a hand-maintained membership check.

### Fixed

- Assistant lookups no longer resolve an arbitrary valid UUID to the singleton
  assistant: only a supported id, the configured default id, or the assistant's
  deterministic UUID match; any other id returns 404.
- The `xray` (`/graph`) and `recurse` (`/subgraphs`) query parameters are now
  forwarded to LangGraph instead of being silently ignored, so the documented
  behaviour matches what the endpoints do.
- Corrected the `from_langgraph_json` module docstring: `store.uri` and
  `http.cors` are consumed; `http.app`, `auth`, and `ui` are ignored with a
  warning (previously it wrongly listed `store` as ignored and omitted `ui`).
- Output-schema filtering now fails closed: when a graph's declared output
  schema cannot be introspected, all state values are dropped (and the event is
  logged) instead of being passed through, preventing internal pipeline fields
  from leaking to API clients via thread state, history, and streaming.
- A checkpoint-read failure when building a thread response now preserves the
  thread's stored status and logs a full traceback, instead of masking every
  failure as `status="error"` with empty values.
- `RunEnrichingCheckpointer` no longer copies the inner saver's `__dict__` over
  its own initialised state; it now initialises a single saver over the shared
  connection and delegates reads to the base class, removing two savers sharing
  mutable connection state.
- Streaming runs no longer replay already-sent output when a transient error
  occurs mid-stream; retries are now confined to the window before the first
  event reaches the client, preventing duplicated output and double model
  invocations.
- The `reject`/`rollback`/`interrupt` multitask strategies are now enforced for
  streaming runs: the thread lock is acquired before the run row is created,
  closing a race where concurrent streaming requests could all start and persist
  orphan `pending` rows.
- Client disconnects during a streaming run (`CancelledError`) are no longer
  swallowed by the retry loop; the run is marked `interrupted` and the thread
  lock is released.
- A failed run's error-state persistence is now best-effort and never masks the
  original exception or prevents the client from receiving the `error` event.
- Token usage for synchronous runs is now read while the thread lock is held, so
  an enqueued run can no longer report another run's totals; checkpoint-read
  failures during usage accounting are logged at error level.
- Run failures now log a full traceback (`exc_info`) instead of just the message.
- Threaded the `ThreadIfExists`/`RunIfNotExists`/`RunStatus` literal types through
  the metadata-store protocol and both implementations instead of widening them
  to bare `str` at the boundary, so mypy now verifies these closed value sets end
  to end. Removed the duplicated `_RUN_LIST_STATUSES` shadow constant.

## [0.1.0] - 2026-06-06

### Added

- Initial public release.
- `create_app(graphs={...}, settings=...)` — assemble a FastAPI app exposing a
  LangGraph Studio-compatible REST surface over any user-supplied graph.
- `from_langgraph_json("langgraph.json")` — load a `langgraph.json` and build the app.
- `SkeinoSettings` — typed configuration (persistence, assistant identity, streaming,
  server presentation, CORS).
- `GraphRegistry` — multi-graph registry (single-graph routed in v1).
- Pluggable checkpointer registry with Postgres and in-memory implementations.
- Endpoints: threads, runs (incl. streaming/SSE), assistants, health/info.

[Unreleased]: https://github.com/ranjitjana027/skeino/compare/v3.1.0...HEAD
[3.1.0]: https://github.com/ranjitjana027/skeino/compare/v3.0.1...v3.1.0
[3.0.1]: https://github.com/ranjitjana027/skeino/compare/v3.0.0...v3.0.1
[3.0.0]: https://github.com/ranjitjana027/skeino/compare/v2.2.0...v3.0.0
[2.2.0]: https://github.com/ranjitjana027/skeino/compare/v2.1.1...v2.2.0
[2.1.1]: https://github.com/ranjitjana027/skeino/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/ranjitjana027/skeino/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/ranjitjana027/skeino/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/ranjitjana027/skeino/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/ranjitjana027/skeino/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/ranjitjana027/skeino/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/ranjitjana027/skeino/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/ranjitjana027/skeino/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/ranjitjana027/skeino/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ranjitjana027/skeino/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ranjitjana027/skeino/releases/tag/v0.1.0
