# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/ranjitjana027/skeino/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ranjitjana027/skeino/releases/tag/v0.1.0
