# Persistence & checkpointers

skeino persists two distinct kinds of data, through two independent mechanisms:

- **Graph state** (LangGraph checkpoints) — through a **checkpointer**.
- **Thread & run metadata** — through a **metadata store**.

Both are chosen by a single setting — **`checkpointer_scheme`** — which defaults
to `memory`. The **scheme alone** selects the backend; `checkpointer_uri` is just
the connection string for that scheme (a URI with no matching scheme is ignored).
All database backends are **optional extras**, imported lazily, so a default
install only ships the in-memory checkpointer.

## Checkpointers

A checkpointer is a LangGraph `BaseCheckpointSaver` that stores the graph's state
snapshots. skeino resolves one at startup from `checkpointer_scheme` and passes
it to your graph builder.

### The registry

Checkpointers are looked up by **scheme** through a small decorator-based
registry:

| Scheme(s) | Implementation | Persists? | Install |
| --- | --- | --- | --- |
| `memory` (default) | LangGraph `MemorySaver` | No (in-process) | bundled |
| `postgres`, `postgresql` | `AsyncPostgresSaver` (run-stamped — see below) | Yes | `skeino[postgres]` |
| `sqlite`, `sqlite3` | `AsyncSqliteSaver` | Yes (file) | `skeino[sqlite]` |
| `mongodb`, `mongo` | `MongoDBSaver` | Yes | `skeino[mongodb]` |
| `redis` | `AsyncRedisSaver` | Yes | `pip install langgraph-checkpoint-redis` ¹ |

¹ Redis isn't a managed skeino extra (`langgraph-checkpoint-redis` caps Python at
`<3.14`); install it yourself. Every DB builder imports its driver lazily and
raises a clear error if the extra is missing.

**The scheme decides the backend — full stop.** `checkpointer_scheme="memory"`
with a `checkpointer_uri="postgresql://…"` still uses in-memory; you must set the
scheme to `postgres` to use Postgres.

### Run-stamped checkpoints

The Postgres checkpointer is wrapped so that, before each checkpoint is written,
the current `run_id` is copied into the checkpoint metadata — that's what lets
clients (and Studio) group a thread's checkpoints by the run that produced them.
Other backends are used as-is, so this run-grouping is **Postgres-only** today.

!!! note "MongoDB specifics"
    The MongoDB checkpointer (`MongoDBSaver`) is backed by a **synchronous**
    pymongo client, so its checkpoint I/O runs on the event loop — size
    accordingly under high concurrency. Name a database in the URI path
    (`mongodb://host/mydb`) and both the checkpointer and the metadata store
    use it; a pathless URI falls back to the historical split defaults
    (`checkpointing_db` for checkpoints, `skeino` for metadata).

### Registering a custom checkpointer

Because lookups are scheme-based, you can add a backend without modifying skeino.
Register an async-context-manager builder against one or more schemes:

```python
from contextlib import asynccontextmanager
from skeino.persistence import register_checkpointer, CheckpointerSpec

@register_checkpointer("mydb")
@asynccontextmanager
async def build_mydb(spec: CheckpointerSpec):
    # spec.scheme, spec.uri, spec.options are available here.
    async with open_my_saver(spec.uri) as saver:
        yield saver
```

Then select it: `SkeinoSettings(checkpointer_scheme="mydb", checkpointer_uri="…")`.
A full walk-through is in
[Write a custom checkpointer](../guides/custom-checkpointer.md).

## The metadata store

Thread and run rows — status, metadata, config, kwargs, TTL, errors — live in a
**metadata store**, separate from the checkpointer. It **follows the same
scheme**, and native implementations exist for the durable schemes:

- **`MetadataStore`** (`postgres`) — two tables, `app_threads` and `app_runs`
  (the latter `ON DELETE CASCADE`), a fresh async connection per operation.
- **`SqliteMetadataStore`** (`sqlite`) — the same two tables over `aiosqlite`
  (a single shared connection, WAL mode + busy timeout so it can share a file
  with the SQLite checkpointer); a durable, serverless option.
- **`MongoMetadataStore`** (`mongodb`) — the same data as two collections over
  `motor`, in the database named by the URI path (else `skeino`).
- **`InMemoryMetadataStore`** — used for `memory`, and for durable checkpointer
  schemes that have no native metadata store (e.g. `redis` or a custom backend).

Every implementation returns the same row shapes, declared as the `ThreadRow` /
`RunRow` TypedDicts next to `MetadataStoreProtocol` in `skeino.persistence` —
the contract a custom backend must satisfy.

!!! warning "No split-brain"
    A durable checkpointer with no native metadata store (e.g. `redis`, or a
    custom scheme) would persist graph state while the thread/run list lived only
    in memory — vanishing on restart. skeino **fails loudly at startup** in that
    case; pick a scheme with a native metadata store (postgres/sqlite/mongodb),
    or pass `allow_ephemeral_metadata=True` to opt in.

## Choosing a setup

| You want… | Use |
| --- | --- |
| Quick local dev, tests, throwaway demos | Default — `checkpointer_scheme="memory"` |
| Durable, serverless (single node, a file) | `checkpointer_scheme="sqlite"`, `checkpointer_uri="/data/skeino.db"` (`skeino[sqlite]`) |
| State shared across workers / a managed DB | `checkpointer_scheme="postgres"`, `checkpointer_uri="postgresql://…"` (`skeino[postgres]`) |
| MongoDB | `checkpointer_scheme="mongodb"`, `checkpointer_uri="mongodb://…/mydb"` (`skeino[mongodb]`) |

!!! warning "In-memory is not for production"
    The in-memory checkpointer and metadata store keep everything in the
    process. They're ideal for development and tests, but all threads, runs, and
    state vanish on restart and are not shared across workers. Use a durable
    scheme for anything real.

See [Set up Postgres persistence](../guides/postgres.md) for a concrete setup.
