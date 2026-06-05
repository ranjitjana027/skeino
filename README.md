# skeino

A reusable, embeddable replacement for the `langgraph dev` HTTP server.

`skeino` exposes the LangGraph Studio-compatible REST surface
(threads, runs, streaming, assistants) over any user-supplied LangGraph
graph. It is built to be:

- **Modular** — `api`, `ops`, `persistence`, `streaming`, `serialization`
  are separate concerns with explicit dependencies.
- **Pluggable** — checkpointers register themselves via a small
  decorator-based registry; Postgres and in-memory ship out of the box,
  Redis / Mongo / etc. can be added without touching `skeino` core.
- **Two entry points** — programmatic `create_app(graphs={...})` and a
  high-level `from_langgraph_json("langgraph.json")` loader.

## Quickstart

```python
# Programmatic
from skeino import create_app, SkeinoSettings
from my_project.graph import graph

app = create_app(
    graphs={"my_agent": graph},
    settings=SkeinoSettings(postgres_uri="postgresql://..."),
)
```

```python
# langgraph.json driven
from skeino import from_langgraph_json
app = from_langgraph_json("langgraph.json")
```

Run with `uvicorn`:

```bash
uvicorn app:app --reload --port 8000
```

## Endpoints (v1)

- `GET /info`, `GET /api/health`, `GET /api/initial-message`
- `POST /assistants/search`, `GET /assistants/{id}`,
  `GET /assistants/{id}/{schemas|graph|subgraphs}`
- `POST /threads`, `GET /threads/{id}`, `POST /threads/search`,
  `GET /threads/{id}/state`, `GET|POST /threads/{id}/history`
- `POST /threads/{id}/runs`, `POST /threads/{id}/runs/stream`,
  `GET /threads/{id}/runs`, `GET /threads/{id}/runs/{run_id}`

Out of scope for v1: `/store/*`, `/runs/crons`, webhooks, auth.

## Status

Pre-release. API may change.
