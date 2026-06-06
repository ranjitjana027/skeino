# Set up Postgres persistence

By default skeino keeps everything in memory, which is great for development but
loses all threads, runs, and state on restart. Point it at PostgreSQL to get
durable, shareable persistence.

## 1. Provision a database

Any reachable PostgreSQL instance works. For local development:

```bash
docker run --name skeino-pg -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 -d postgres:16
```

Create a database (or reuse `postgres`):

```bash
createdb -h localhost -U postgres skeino
```

## 2. Point skeino at it

Set `postgres_uri` on your settings. That single field switches **both** the
checkpointer and the metadata store to Postgres:

```python title="app.py"
from skeino import create_app, SkeinoSettings
from my_project.graph import graph

app = create_app(
    graphs={"my_agent": graph},
    settings=SkeinoSettings(
        postgres_uri="postgresql://postgres:postgres@localhost:5432/skeino",
    ),
)
```

Or, with a `langgraph.json` manifest, via the `store.uri` field (which supports
environment expansion):

```json title="langgraph.json"
{
  "env": ".env",
  "graphs": { "my_agent": "./src/graph.py:graph" },
  "store": { "uri": "${POSTGRES_URI}" }
}
```

```bash title=".env"
POSTGRES_URI=postgresql://postgres:postgres@localhost:5432/skeino
```

## 3. Schema creation

On startup skeino creates what it needs:

- the **metadata** tables `app_threads` and `app_runs` (the latter references the
  former with `ON DELETE CASCADE`), and
- the **LangGraph checkpoint** tables, via the Postgres saver's `setup()`.

This happens automatically the first time the app boots against the database. If
you manage schema migrations yourself and want to suppress the checkpointer's
auto-setup, pass `checkpointer_options={"setup_schema": False}` (and create the
checkpoint tables out of band).

## 4. Let skeino compile your graph against the checkpointer

If your graph needs the checkpointer at compile time, register a **builder**
instead of a precompiled graph. skeino resolves the Postgres checkpointer and
hands it in:

```python
async def build_graph(checkpointer):
    builder = StateGraph(...)
    return builder.compile(checkpointer=checkpointer)

app = create_app(
    graphs={"my_agent": build_graph},
    settings=SkeinoSettings(postgres_uri="postgresql://localhost/skeino"),
)
```

## Connection notes

- skeino depends on `psycopg` v3 (with the `binary` and `pool` extras), so no
  separate driver install is needed.
- Use a connection string your network can reach from the server process. For
  managed Postgres, include `sslmode=require` (or your provider's equivalent) in
  the URI.

## Verifying durability

Start the server, create a thread and run a graph, then restart the process.
`GET /threads/{id}/state` should still return the prior state — confirming
checkpoints survived the restart. With the in-memory default, the thread would
be gone.

See [Persistence & checkpointers](../concepts/persistence.md) for the full
model, and [Write a custom checkpointer](custom-checkpointer.md) for non-Postgres
backends.
