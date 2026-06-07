# Write a custom checkpointer

skeino selects a checkpointer by **URI scheme** from a registry. Postgres and
in-memory ship built in; you can register any other backend (Redis, Mongo,
SQLite, …) without modifying skeino, as long as it can produce a LangGraph
`BaseCheckpointSaver`.

## The builder contract

A checkpointer builder is an **async context manager factory** keyed to one or
more schemes. It receives a [`CheckpointerSpec`][skeino.persistence.CheckpointerSpec]
(the resolved `scheme`, `uri`, and `options`) and yields a `BaseCheckpointSaver`,
cleaning up its resources on exit.

```python
from contextlib import asynccontextmanager
from langgraph.checkpoint.base import BaseCheckpointSaver
from skeino.persistence import register_checkpointer, CheckpointerSpec

@register_checkpointer("redis")            # one or more schemes
@asynccontextmanager
async def build_redis(spec: CheckpointerSpec):
    setup = bool(spec.options.get("setup_schema", True))
    async with open_redis_saver(spec.uri) as saver:   # your saver
        if setup and hasattr(saver, "setup"):
            await saver.setup()
        yield saver
```

Key points:

- The decorator order matters: `@register_checkpointer(...)` wraps the
  `@asynccontextmanager`-decorated function.
- `spec.uri` is the connection string (may be `None` — handle that if your
  backend requires one).
- `spec.options` carries `checkpointer_options` from settings, plus the
  `setup_schema` flag skeino threads through. Respect it so callers who manage
  their own schema can disable auto-setup.
- Register multiple schemes by passing several strings:
  `@register_checkpointer("redis", "rediss")`.

## Selecting it

Once registered (i.e. the module defining the builder is imported), select the
scheme — the **scheme** decides the backend, and `checkpointer_uri` is its
connection string:

```python
from skeino import create_app, SkeinoSettings

app = create_app(
    graphs={"my_agent": build_graph},
    settings=SkeinoSettings(
        checkpointer_scheme="redis",
        checkpointer_uri="redis://localhost:6379/0",
        checkpointer_options={"setup_schema": False, "ttl": 3600},
    ),
)
```

!!! warning "Import the module that registers the builder"
    Registration is a side effect of the decorator running, so the module
    containing your `@register_checkpointer` builder must be imported before
    `create_app` resolves the checkpointer. Importing it in the same module that
    calls `create_app` (or your package's `__init__`) is enough.

## Resolution

`SkeinoSettings.checkpointer_scheme` selects the backend directly (default
`memory`); `checkpointer_uri` is just its connection string. If no builder is
registered for the scheme, skeino raises a `ValueError` listing the known
schemes — so a typo'd scheme fails loudly at startup rather than silently
falling back.

## A note on metadata persistence

The registry covers the **checkpointer** (graph state). Thread/run **metadata**
uses a separate store that follows the same scheme — native for
`postgres`/`sqlite`/`mongodb`, in-memory otherwise. A durable custom checkpointer
without a native metadata store trips the startup **fail-loud** guard unless you
supply your own [`MetadataStoreProtocol`][skeino.persistence.MetadataStoreProtocol]
or set `allow_ephemeral_metadata=True`. See
[Persistence & checkpointers](../concepts/persistence.md).
