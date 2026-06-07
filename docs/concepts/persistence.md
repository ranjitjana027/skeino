# Persistence & checkpointers

skeino persists two distinct kinds of data, through two independent mechanisms:

- **Graph state** (LangGraph checkpoints) — through a **checkpointer**.
- **Thread & run metadata** — through a **metadata store**.

Both are selected from your [`SkeinoSettings`](configuration.md). With a
`postgres_uri`, both use Postgres; with a `sqlite_path` (and no `postgres_uri`),
both use SQLite — a serverless durable option; without either, both fall back to
ephemeral in-memory implementations.

## Checkpointers

A checkpointer is a LangGraph `BaseCheckpointSaver` that stores the graph's state
snapshots. skeino resolves one at startup and either passes it to your graph
builder or uses it to read state for the API.

### The registry

Checkpointers are looked up by **URI scheme** through a small decorator-based
registry. Two ship out of the box:

| Scheme(s) | Implementation | Persists? | Install |
| --- | --- | --- | --- |
| `postgres`, `postgresql` | `AsyncPostgresSaver` (wrapped — see below) | Yes | bundled |
| `sqlite`, `sqlite3` | `AsyncSqliteSaver` | Yes (file) | `skeino[sqlite]` |
| `redis` | `AsyncRedisSaver` | Yes | `pip install langgraph-checkpoint-redis` ¹ |
| `memory` | LangGraph `MemorySaver` | No (in-process) | bundled |

¹ Redis isn't a managed skeino extra (`langgraph-checkpoint-redis` caps Python at
`<3.14`); install it yourself. The builder imports it lazily and raises a clear
error if it's missing.

Scheme resolution at startup:

1. `SkeinoSettings.checkpointer_scheme` if set, else
2. `sqlite` when `sqlite_path` is set, else
3. the scheme parsed from `postgres_uri`, else
4. `memory`.

### Run-stamped checkpoints

The Postgres checkpointer is wrapped in a `RunEnrichingCheckpointer`. Before each
checkpoint is written, it copies the current `run_id` into the checkpoint
metadata. That's what lets clients (and Studio) group a thread's checkpoints by
the run that produced them. The in-memory saver is used as-is.

### Registering a custom checkpointer

Because lookups are scheme-based, you can add a backend (Redis, Mongo, …)
without modifying skeino. Register an async-context-manager builder against one
or more schemes:

```python
from contextlib import asynccontextmanager
from skeino.persistence import register_checkpointer, CheckpointerSpec

@register_checkpointer("redis")
@asynccontextmanager
async def build_redis(spec: CheckpointerSpec):
    # spec.scheme, spec.uri, spec.options are available here.
    async with open_my_redis_saver(spec.uri) as saver:
        yield saver
```

Then point skeino at it via settings:

```python
SkeinoSettings(
    postgres_uri="redis://localhost:6379/0",   # scheme drives selection
    # or, to be explicit:
    checkpointer_scheme="redis",
    checkpointer_options={"setup_schema": False},
)
```

A full walk-through is in
[Write a custom checkpointer](../guides/custom-checkpointer.md).

## The metadata store

Thread and run rows — status, metadata, config, kwargs, TTL, errors — live in a
**metadata store**, separate from the checkpointer. Two implementations satisfy
the same protocol:

- **`MetadataStore`** (Postgres) — used when `postgres_uri` is set. It creates
  two tables on startup, `app_threads` and `app_runs` (the latter referencing the
  former with `ON DELETE CASCADE`), and opens a fresh async connection per
  operation.
- **`SqliteMetadataStore`** (SQLite) — used when `sqlite_path` is set (and no
  `postgres_uri`). Same two tables over `aiosqlite` (a single shared connection),
  for a durable, serverless deployment. Requires the `skeino[sqlite]` extra.
- **`InMemoryMetadataStore`** — used otherwise. State lives in process and is
  lost on restart; skeino logs a warning so this isn't a silent surprise.

The metadata store always follows the same backend as durable persistence: set
`postgres_uri` **or** `sqlite_path` and both the checkpointer and metadata store
use it. Any object implementing the metadata-store protocol can be supplied for
advanced use.

!!! warning "No split-brain"
    If you configure a *durable* `checkpointer_scheme` (e.g. a custom backend)
    without a `postgres_uri`/`sqlite_path`, the metadata store would be in-memory
    while graph state persists — the thread/run list would vanish on restart
    while state survives. skeino **fails loudly at startup** in that case; set a
    durable metadata backend or pass `allow_ephemeral_metadata=True` to opt in.

## Choosing a setup

| You want… | Use |
| --- | --- |
| Quick local dev, tests, throwaway demos | Default (in-memory) — no `postgres_uri`/`sqlite_path` |
| Durable, serverless (single node, a file) | `sqlite_path="/data/skeino.db"` (`skeino[sqlite]`) |
| State shared across workers / a managed DB | `postgres_uri="postgresql://…"` |
| A different backend (Redis, Mongo, …) | A [custom checkpointer](../guides/custom-checkpointer.md) + a durable metadata store |

!!! warning "In-memory is not for production"
    The in-memory checkpointer and metadata store keep everything in the
    process. They're ideal for development and tests, but all threads, runs, and
    state vanish on restart and are not shared across workers. Use Postgres or
    SQLite for anything durable.

See [Set up Postgres persistence](../guides/postgres.md) for a concrete setup.
