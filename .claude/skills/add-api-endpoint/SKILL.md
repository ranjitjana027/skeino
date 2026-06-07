---
name: add-api-endpoint
description: >-
  Scaffold a new HTTP endpoint or API feature in the skeino server across all
  its layers (schema → ops → route → persistence both backends → FakeGraph →
  tests → changelog → docs) and open a PR. Use when adding or extending an
  endpoint/feature — especially the LangGraph server-API parity work (threads,
  runs, assistants, store, crons) — so the change follows skeino's layered
  conventions and definition of done.
---

# Add an API endpoint / feature to skeino

A deterministic loop for shipping a server-API change. skeino layers a request
as `api/` (thin) → `ops/` (logic) → `persistence/` + `streaming/`, with pydantic
`schemas/`. Thread the change through in order. See `CLAUDE.md` for architecture.

## 0. Branch & issue

- `git checkout main && git pull`, then `git checkout -b feat/<slug>` (or
  `fix/<slug>`). Branch off latest `main`, never off another open branch.
- For a feature, ensure a tracking issue exists (Problem/motivation → Proposed
  solution → Design questions → Relevant code). The PR will `Closes #N`.

## 1. Implement across the layers (in order)

1. **Schema** — add request/response models in `src/skeino/schemas/<area>.py`
   and export them in `src/skeino/schemas/__init__.py` (both the import and
   `__all__`). Reuse the shared `Literal` aliases in `schemas/common.py`
   (`RunStatus`, `ThreadIfExists`, …); don't widen to bare `str`.
2. **Ops** — add the method to `RunOps`/`ThreadOps`/`AssistantOps` in
   `src/skeino/ops/`. This is where validation (404/409/422 via `HTTPException`),
   locking, and graph/store calls live. Read graph state via
   `graph.aget_state` / write via `graph.aupdate_state`; build configs with
   `serialization.build_thread_config`.
3. **Route** — add the endpoint in `src/skeino/api/<area>.py`. Keep it thin:
   `parse_request_model(...)`, `get_state(request)`, call the op, return.
   Register literal subpaths before `{param}` paths.
4. **Persistence** — if you touch storage, update the
   `MetadataStoreProtocol` in `persistence/base.py` **and both**
   `persistence/metadata_store.py` (Postgres; mirror the existing
   `async with await psycopg.AsyncConnection.connect(self._postgres_uri)`
   pattern, bind every value via `%s`) **and** `persistence/in_memory_store.py`.
   Keep the two implementations behaviourally identical.
5. **Checkpointer** — graph state/history lives in `graph.checkpointer`
   (has `adelete_thread`, etc.); guard with `getattr` so it's optional.

## 2. Extend FakeGraph only as needed

`tests/conftest.py::FakeGraph` is the fake compiled graph + `_FakeCheckpointer`.
If the feature needs behaviour the fake can't yet exercise (failure injection,
cancellation, checkpoint selection, list-valued state, …), extend it — otherwise
the tests will pass vacuously. It already supports per-thread state, ordered
checkpoints by id, and a deletable checkpointer.

## 3. Tests (behavioural, non-vacuous)

- Integration tests via `build_test_app` / `TestClient` in
  `tests/integration/`; assert response shape/status, not internals.
- `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` needed.
- Make tests **fail if the feature breaks**: e.g. for selection, write two
  distinct states and assert the selected one is returned; for fail-closed
  behaviour, assert the leak does NOT happen.
- Cover the unhappy paths: 404 (missing), 409 (conflict), 422 (validation).

## 4. Changelog & docs

- Add a **changelog fragment** `changelog.d/<id>.<type>.md` (type ∈
  added/changed/fixed/…) — one file per change, not a `CHANGELOG.md` edit. See
  `changelog.d/README.md`.
- `docs/api-reference/http.md` — add the route to the relevant table.
- `docs/concepts/*.md` — a sentence in the matching concept page.

## 5. Verify, commit, PR

```bash
poetry run ruff format . && poetry run ruff check . \
  && poetry run mypy src && poetry run bandit -r src && poetry run pytest
```

Commit only the files for this change (leave unrelated working-tree edits like
`.claude/settings.json` alone). Push and open the PR with
`.github/PULL_REQUEST_TEMPLATE.md`, `Closes #N`, and a behaviour-change note if
any. Then use the `respond-to-pr-review` skill for review feedback.

## Scope notes

- v1 is single-graph; `after_seconds`/`webhook` are intentionally rejected;
  `store`/`auth`/`ui` from `langgraph.json` are not fully wired. Don't silently
  expand scope — check the open parity issues.
