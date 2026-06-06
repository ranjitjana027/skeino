# Persistence & checkpointers

skeino persists two distinct kinds of data, through two independent mechanisms:

- **Graph state** (LangGraph checkpoints) — through a **checkpointer**.
- **Thread & run metadata** — through a **metadata store**.

Both are selected from your [`SkeinoSettings`](configuration.md). With a
`postgres_uri`, both use Postgres; without one, both fall back to ephemeral
in-memory implementations.

## Checkpointers

A checkpointer is a LangGraph `BaseCheckpointSaver` that stores the graph's state
snapshots. skeino resolves one at startup and either passes it to your graph
builder or uses it to read state for the API.

### The registry

Checkpointers are looked up by **URI scheme** through a small decorator-based
registry. Two ship out of the box:

| Scheme(s) | Implementation | Persists? |
| --- | --- | --- |
| `postgres`, `postgresql` | `AsyncPostgresSaver` (wrapped — see below) | Yes |
| `memory` | LangGraph `MemorySaver` | No (in-process) |

Scheme resolution at startup:

1. `SkeinoSettings.checkpointer_scheme` if set, else
2. the scheme parsed from `postgres_uri`, else
3. `memory`.

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
- **`InMemoryMetadataStore`** — used otherwise. State lives in process and is
  lost on restart; skeino logs a warning so this isn't a silent surprise.

Any object implementing the metadata-store protocol can be supplied for advanced
use, but the two built-ins cover the common cases.

## Choosing a setup

| You want… | Use |
| --- | --- |
| Quick local dev, tests, throwaway demos | Default (in-memory) — no `postgres_uri` |
| State that survives restarts / multiple workers sharing a DB | `postgres_uri="postgresql://…"` |
| A different backend (Redis, Mongo, …) | A [custom checkpointer](../guides/custom-checkpointer.md) |

!!! warning "In-memory is not for production"
    The in-memory checkpointer and metadata store keep everything in the
    process. They're ideal for development and tests, but all threads, runs, and
    state vanish on restart and are not shared across workers. Use Postgres for
    anything durable.

See [Set up Postgres persistence](../guides/postgres.md) for a concrete setup.
