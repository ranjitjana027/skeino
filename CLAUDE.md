# skeino — contributor & agent guide

skeino is a reusable, embeddable replacement for the `langgraph dev` HTTP
server: threads, runs, streaming, and assistants over any LangGraph graph.
It is a **server-API** library, not the LangGraph Cloud control plane.

## Commands

Dev env uses Poetry (Python 3.11–3.13). The five checks below run in CI and
**all must pass** before a PR merges:

```bash
poetry install
poetry run ruff format --check .   # formatting (use `ruff format .` to fix)
poetry run ruff check .            # lint (use `ruff check --fix .`)
poetry run mypy src                # strict types
poetry run bandit -r src           # security
poetry run pytest                  # unit + integration (in-memory, no services)
```

Infra-backed API tests (local-only, not part of the five checks; needs Docker):

```bash
docker compose up -d --wait
poetry install --all-extras --with redis
poetry run pytest tests/api
docker compose down -v
```

## Architecture (request flows through layers, in this order)

```
api/      FastAPI routers — thin; parse request, call ops, shape response
ops/      business logic — RunOps, ThreadOps, AssistantOps
persistence/  metadata store (thread/run rows) + checkpointer resolution
streaming/    SSE encoding, retry, stream-mode dispatch
schemas/      pydantic request/response models + shared Literal aliases
serialization/  graph state <-> wire conversion
```

Two separate persistence backends, by design:

- **Metadata store** (`persistence/base.py` `MetadataStoreProtocol`) — thread/run
  rows (status, metadata, config, ttl, kwargs). Two implementations that must
  stay in lockstep: `MetadataStore` (Postgres) and `InMemoryMetadataStore`.
- **Checkpointer** — LangGraph graph state/history (Postgres saver or
  `MemorySaver`), resolved in `persistence/checkpointer.py`. The graph exposes
  it as `graph.checkpointer`.

`create_app` (`app.py`) wires everything in its lifespan. Supported public
surface: `create_app`, `SkeinoSettings`, `from_langgraph_json`, `GraphRegistry`.
v1 routes a **single graph**.

## Definition of done (every change)

- Behavioural tests added/updated (see below).
- A changelog **fragment** for any user-facing change: `changelog.d/<id>.<type>.md`
  (type ∈ added/changed/deprecated/removed/fixed/security). Do **not** edit
  `CHANGELOG.md` directly — see `changelog.d/README.md`. The version is **not**
  bumped per PR — `cut-release` decides and sets it at release time from the
  accumulated fragments (`__version__` derives from `pyproject.toml`).
- Docs updated where relevant: `docs/api-reference/http.md` (endpoint table) and
  the matching `docs/concepts/*.md`.
- All five checks green. Code fully typed (mypy is strict).
- Errors **fail closed / loud**: never silently swallow exceptions or fall open
  in a way that leaks internal state; log with `exc_info` on failure paths.

## Adding an API endpoint / feature

Thread the change through the layers in order — and when touching persistence,
update **both** the protocol and **both** store implementations:

`schemas/*.py` (export in `schemas/__init__.py`) → `ops/*.py` →
`api/*.py` route → `persistence/base.py` protocol + `metadata_store.py` +
`in_memory_store.py` → extend `tests/conftest.py::FakeGraph` if new
graph/checkpointer behaviour is needed → tests → changelog fragment → docs.

The `add-api-endpoint` skill walks this end to end. To review and land open PRs,
see `review-and-merge-prs`; to ship a release, see `cut-release`.

## Testing conventions

- Tests are self-contained: `FakeGraph` (a fake compiled graph) and
  `build_test_app` in `tests/conftest.py`; no Postgres/LLM required.
- Prefer **integration tests** through `TestClient`; assert on response
  shape/status, not internals. `asyncio_mode = "auto"` — async tests need **no**
  `@pytest.mark.asyncio`.
- Tests must be **non-vacuous**: a test that still passes when the feature is
  broken is worthless. If a behaviour can't be exercised (e.g. failure
  injection, checkpoint selection), extend `FakeGraph` so it can.
- `tests/api/` runs against **real** Postgres/Mongo/Redis from
  `docker-compose.yml` with a real LangGraph echo graph. It is excluded from
  plain `pytest` via `testpaths` (keeps the default suite sub-second) — run it
  explicitly with `poetry run pytest tests/api`; it fails loud when the
  services are down.

## Git / PR workflow

- Branch off the latest `main` (`feat/…` or `fix/…`); don't stack on another
  open branch. Keep PRs file-disjoint where possible.
- Features get a tracking issue first; PR body uses
  `.github/PULL_REQUEST_TEMPLATE.md` and says `Closes #N`.
- User-facing changes add a `changelog.d/` fragment, not a `CHANGELOG.md` edit,
  so concurrent PRs don't conflict on the changelog (collated on release with
  `towncrier build`).

## Known Copilot review false positives

Verified against this codebase — don't "fix" these:

- `logger.error(..., exc_info=<exception instance>)` is valid since Python 3.5
  (it uses the instance's `__traceback__`), even outside an `except` block.
- `asyncio_mode = "auto"` means bare `async def` tests run without
  `@pytest.mark.asyncio`.
