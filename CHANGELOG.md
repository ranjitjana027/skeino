# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Unreleased changes live as [changelog fragments](https://github.com/ranjitjana027/skeino/blob/main/changelog.d/README.md)
under `changelog.d/` and are collated here on release with `towncrier build`.

<!-- towncrier release notes start -->

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

[Unreleased]: https://github.com/ranjitjana027/skeino/compare/v2.0.2...HEAD
[2.0.2]: https://github.com/ranjitjana027/skeino/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/ranjitjana027/skeino/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/ranjitjana027/skeino/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/ranjitjana027/skeino/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/ranjitjana027/skeino/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/ranjitjana027/skeino/compare/v0.3.0...v1.0.0
[0.3.0]: https://github.com/ranjitjana027/skeino/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ranjitjana027/skeino/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ranjitjana027/skeino/releases/tag/v0.1.0
