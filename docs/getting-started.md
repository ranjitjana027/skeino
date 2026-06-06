# Getting started

This page takes you from an empty environment to a running skeino server, using
either the programmatic factory or a `langgraph.json` manifest.

## Requirements

- **Python 3.11, 3.12, or 3.13** (skeino requires `>=3.11,<4.0`).
- A compiled LangGraph graph (`langgraph >= 1.0`).
- Optionally, a **PostgreSQL** database if you want state to survive restarts.

## Install

```bash
pip install skeino
```

skeino pulls in FastAPI, Pydantic v2, LangGraph (+ the Postgres checkpointer),
`psycopg`, and `uvicorn` as runtime dependencies, so a single install gives you
everything needed to serve a graph.

## Path A — Programmatic (`create_app`)

Use [`create_app`][skeino.create_app] when you assemble the application in
Python. It takes a mapping of **assistant id → graph** and a
[`SkeinoSettings`][skeino.SkeinoSettings] object, and returns a standard
`FastAPI` instance.

```python title="app.py"
from langgraph.graph import StateGraph
from skeino import create_app, SkeinoSettings

# 1. Build and compile your graph however you normally would.
builder = StateGraph(...)
graph = builder.compile()

# 2. Hand it to skeino.
app = create_app(
    graphs={"my_agent": graph},
    settings=SkeinoSettings(),  # in-memory persistence by default
)
```

!!! note "Keyword-only arguments"
    `create_app` is keyword-only: call it as
    `create_app(graphs=..., settings=...)`, not positionally.

Run it with uvicorn:

```bash
uvicorn app:app --reload --port 8000
```

The server is now live at `http://localhost:8000`, with interactive OpenAPI docs
at `http://localhost:8000/docs`.

### Letting skeino own the checkpointer

Instead of a precompiled graph, you can pass a **builder** —
`(checkpointer) -> CompiledStateGraph`, sync or async. skeino resolves a
checkpointer from your settings and passes it in, so your graph is compiled
against the same persistence backend skeino uses:

```python
async def build_graph(checkpointer):
    builder = StateGraph(...)
    return builder.compile(checkpointer=checkpointer)

app = create_app(
    graphs={"my_agent": build_graph},
    settings=SkeinoSettings(postgres_uri="postgresql://localhost/skeino"),
)
```

## Path B — `langgraph.json` driven

If you already describe your project with a `langgraph.json` manifest, use
[`from_langgraph_json`][skeino.from_langgraph_json]. It parses the manifest,
loads each graph by its `path:attribute` target, applies CORS and store
settings, and calls `create_app` for you.

```json title="langgraph.json"
{
  "env": ".env",
  "graphs": {
    "my_agent": "./src/graph.py:graph"
  },
  "http": {
    "cors": {
      "allow_origins": ["*"]
    }
  },
  "store": {
    "uri": "${POSTGRES_URI}"
  }
}
```

```python title="src/graph.py"
from langgraph.graph import StateGraph

builder = StateGraph(...)
graph = builder.compile()
```

```python title="app.py"
from skeino import from_langgraph_json

app = from_langgraph_json("langgraph.json")
```

```bash
uvicorn app:app --reload --port 8000
```

Graph targets are resolved **relative to the manifest's directory**, and
`${VAR}` placeholders in values such as `store.uri` are expanded from the
environment (after loading the `env` file, if declared).

See [Configuration](concepts/configuration.md) for the full manifest schema and
every `SkeinoSettings` field.

## Verify it works

With the server running, hit the health and info endpoints:

```bash
curl http://localhost:8000/api/health
# {"status":"ok","version":"1.0.0"}

curl http://localhost:8000/info
# {"status":"ok","name":"skeino","version":"1.0.0"}
```

Then create a thread and start a streaming run:

```bash
# Create a thread
curl -X POST http://localhost:8000/threads -d '{}'

# Stream a run (replace <thread_id>)
curl -N -X POST http://localhost:8000/threads/<thread_id>/runs/stream \
  -H 'Content-Type: application/json' \
  -d '{"assistant_id": "my_agent", "input": {"messages": [{"type": "human", "content": "hello"}]}}'
```

You'll receive a `text/event-stream` response. See
[Streaming (SSE)](concepts/streaming.md) for the event sequence and payload
shapes.

## Next steps

- Understand the data model in [Threads & runs](concepts/threads-and-runs.md).
- Configure durable storage in [Set up Postgres persistence](guides/postgres.md).
- Mount skeino alongside your own routes in
  [Embed in an existing FastAPI app](guides/embedding-fastapi.md).
