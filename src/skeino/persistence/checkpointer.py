"""Pluggable checkpointer resolution for the skeino runtime.

Built-in schemes:

* ``postgres`` / ``postgresql`` — async PostgreSQL saver from
  ``langgraph_checkpoint_postgres``, wrapped with
  :class:`RunEnrichingCheckpointer` so LangGraph Studio groups checkpoints
  by run.
* ``memory`` — in-process saver from ``langgraph.checkpoint.memory``.
  Used when no URI is configured.

Additional backends (Redis, MongoDB, etc.) can plug themselves in:

.. code-block:: python

    from skeino.persistence import register_checkpointer

    @register_checkpointer("redis")
    def _build_redis(spec: CheckpointerSpec) -> AsyncContextManager[BaseCheckpointSaver]:
        ...

Each builder is an async context manager so connection lifetimes follow
``AsyncExitStack`` semantics — exactly one place to release resources.
"""

import inspect
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncContextManager, AsyncIterator, Callable

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.memory import MemorySaver

from skeino.persistence.enriching import build_run_enriching_checkpointer

CheckpointerBuilder = Callable[
    ["CheckpointerSpec"], AsyncContextManager[BaseCheckpointSaver]
]


@dataclass(frozen=True)
class CheckpointerSpec:
    """Declarative request for a checkpointer instance."""

    scheme: str
    uri: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


_REGISTRY: dict[str, CheckpointerBuilder] = {}


def register_checkpointer(
    *schemes: str,
) -> Callable[[CheckpointerBuilder], CheckpointerBuilder]:
    """Register a checkpointer builder for one or more URI schemes."""

    def decorator(builder: CheckpointerBuilder) -> CheckpointerBuilder:
        for scheme in schemes:
            _REGISTRY[scheme] = builder
        return builder

    return decorator


def _scheme_for_uri(uri: str | None) -> str:
    """Return the scheme to use given a URI (or ``memory`` when missing)."""
    if not uri:
        return "memory"
    head, _, _ = uri.partition("://")
    return head.lower() or "memory"


@asynccontextmanager
async def open_checkpointer(
    uri: str | None = None,
    *,
    scheme: str | None = None,
    setup_schema: bool = True,
    options: dict[str, Any] | None = None,
) -> AsyncIterator[BaseCheckpointSaver]:
    """Yield a checkpointer instance, releasing its resources on exit.

    Resolution: an explicit ``scheme`` wins; otherwise it is derived from the
    ``uri``; falling back to ``memory`` when both are absent.
    """
    effective_scheme = (scheme or _scheme_for_uri(uri)).lower()
    builder = _REGISTRY.get(effective_scheme)
    if builder is None:
        raise ValueError(
            f"No checkpointer registered for scheme {effective_scheme!r}. "
            f"Known schemes: {sorted(_REGISTRY)}"
        )
    spec = CheckpointerSpec(
        scheme=effective_scheme,
        uri=uri,
        options={"setup_schema": setup_schema, **(options or {})},
    )
    async with builder(spec) as checkpointer:
        yield checkpointer


# ---------------------------------------------------------------------------
# Built-in builders
# ---------------------------------------------------------------------------


@register_checkpointer("postgres", "postgresql")
@asynccontextmanager
async def _build_postgres(spec: CheckpointerSpec) -> AsyncIterator[BaseCheckpointSaver]:
    """Build an enrichment-wrapped async PostgreSQL checkpointer.

    Requires the ``skeino[postgres]`` extra (psycopg + langgraph-checkpoint-
    postgres), imported lazily so postgres stays optional.
    """
    if not spec.uri:
        raise ValueError("Postgres checkpointer requires a connection URI.")
    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "The 'postgres' checkpointer requires the skeino[postgres] extra "
            "(pip install 'skeino[postgres]')."
        ) from exc

    setup_schema = bool(spec.options.get("setup_schema", True))
    async with AsyncExitStack() as stack:
        saver_cm = AsyncPostgresSaver.from_conn_string(spec.uri)
        inner = await stack.enter_async_context(saver_cm)
        if setup_schema and hasattr(inner, "setup"):
            result = inner.setup()
            if inspect.isawaitable(result):
                await result
        yield build_run_enriching_checkpointer(inner)


@register_checkpointer("mongodb", "mongo")
@asynccontextmanager
async def _build_mongodb(spec: CheckpointerSpec) -> AsyncIterator[BaseCheckpointSaver]:
    """Build an async MongoDB checkpointer (requires the ``skeino[mongodb]`` extra)."""
    if not spec.uri:
        raise ValueError("MongoDB checkpointer requires a connection URI.")
    try:
        from langgraph.checkpoint.mongodb import MongoDBSaver
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "The 'mongodb' checkpointer requires the skeino[mongodb] extra "
            "(pip install 'skeino[mongodb]')."
        ) from exc

    # MongoDBSaver.from_conn_string is a *sync* context manager that exposes the
    # async checkpoint methods skeino uses.
    with MongoDBSaver.from_conn_string(spec.uri) as saver:
        yield saver


@register_checkpointer("memory")
@asynccontextmanager
async def _build_memory(spec: CheckpointerSpec) -> AsyncIterator[BaseCheckpointSaver]:
    """Build an in-process MemorySaver. URI is ignored."""
    del spec
    yield MemorySaver()


def _sqlite_conn_string(uri: str | None) -> str:
    """Normalise a sqlite URI/path to what AsyncSqliteSaver expects."""
    if not uri:
        return ":memory:"
    for prefix in ("sqlite:///", "sqlite://"):
        if uri.startswith(prefix):
            return uri[len(prefix) :] or ":memory:"
    return uri


@register_checkpointer("sqlite", "sqlite3")
@asynccontextmanager
async def _build_sqlite(spec: CheckpointerSpec) -> AsyncIterator[BaseCheckpointSaver]:
    """Build an async SQLite checkpointer (requires the ``skeino[sqlite]`` extra)."""
    try:
        from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "The 'sqlite' checkpointer requires the skeino[sqlite] extra "
            "(pip install 'skeino[sqlite]')."
        ) from exc

    conn_string = _sqlite_conn_string(spec.uri)
    setup_schema = bool(spec.options.get("setup_schema", True))
    async with AsyncExitStack() as stack:
        saver = await stack.enter_async_context(
            AsyncSqliteSaver.from_conn_string(conn_string)
        )
        if setup_schema and hasattr(saver, "setup"):
            result = saver.setup()
            if inspect.isawaitable(result):
                await result
        yield saver


@register_checkpointer("redis")
@asynccontextmanager
async def _build_redis(spec: CheckpointerSpec) -> AsyncIterator[BaseCheckpointSaver]:
    """Build an async Redis checkpointer.

    Requires ``langgraph-checkpoint-redis``, which is not a managed skeino extra
    (it caps Python at <3.14) — install it yourself:
    ``pip install langgraph-checkpoint-redis``.
    """
    if not spec.uri:
        raise ValueError("Redis checkpointer requires a connection URI.")
    try:
        from langgraph.checkpoint.redis.aio import AsyncRedisSaver
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "The 'redis' checkpointer requires langgraph-checkpoint-redis "
            "(pip install langgraph-checkpoint-redis)."
        ) from exc

    setup_schema = bool(spec.options.get("setup_schema", True))
    async with AsyncExitStack() as stack:
        saver = await stack.enter_async_context(
            AsyncRedisSaver.from_conn_string(spec.uri)
        )
        if setup_schema:
            for setup_name in ("asetup", "setup"):
                setup_fn = getattr(saver, setup_name, None)
                if setup_fn is not None:
                    result = setup_fn()
                    if inspect.isawaitable(result):
                        await result
                    break
        yield saver
