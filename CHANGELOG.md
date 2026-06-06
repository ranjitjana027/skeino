# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
