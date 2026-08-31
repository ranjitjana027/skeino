"""Pluggable checkpointer resolution for the skeino runtime.

The backend is chosen by **scheme** (``SkeinoSettings.checkpointer_scheme``,
default ``memory``); the URI is only a connection string. Built-in schemes —
the database ones import their driver lazily and need the matching extra:

* ``memory`` (default) — in-process ``MemorySaver`` (bundled).
* ``postgres`` / ``postgresql`` — async PostgreSQL saver. Extra:
  ``skeino[postgres]``.
* ``sqlite`` / ``sqlite3`` — ``AsyncSqliteSaver``. Extra: ``skeino[sqlite]``.
* ``mongodb`` / ``mongo`` — ``MongoDBSaver``. Extra: ``skeino[mongodb]``.
* ``redis`` — ``AsyncRedisSaver`` (install ``langgraph-checkpoint-redis``
  yourself; it isn't a managed extra).

The postgres, sqlite, and redis savers are wrapped via
:func:`skeino.persistence.enriching.build_run_enriching_checkpointer` so each
checkpoint carries its ``run_id`` and LangGraph Studio groups checkpoints by
run. MongoDB's saver merges config metadata (and thus ``run_id``) natively, so
it is left unwrapped.

Additional backends can plug themselves in under a new scheme:

.. code-block:: python

    from skeino.persistence import register_checkpointer

    @register_checkpointer("mydb")
    def _build_mydb(spec: CheckpointerSpec) -> AsyncContextManager[BaseCheckpointSaver]:
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
from skeino.persistence.uri import mongo_db_from_uri, normalize_sqlite_uri

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

    The saver runs over an :class:`~psycopg_pool.AsyncConnectionPool` rather
    than a single ``from_conn_string`` connection: a connection dropped by the
    server or a connection pooler (e.g. a Supabase/pgbouncer idle-timeout or
    recycle) is then detected and replaced on checkout, instead of wedging
    *every* subsequent checkpoint read with ``OperationalError: the connection
    is closed``. ``check=check_connection`` validates a connection before each
    checkout; ``prepare_threshold=0`` disables client-side prepared statements,
    which also keeps the saver correct behind a transaction-mode pooler.

    ``pool_max_size`` (default 10) is read from ``spec.options``.
    """
    if not spec.uri:
        raise ValueError("Postgres checkpointer requires a connection URI.")
    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
        from psycopg import AsyncConnection
        from psycopg.rows import DictRow, dict_row
        from psycopg_pool import AsyncConnectionPool
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "The 'postgres' checkpointer requires the skeino[postgres] extra "
            "(pip install 'skeino[postgres]')."
        ) from exc

    setup_schema = bool(spec.options.get("setup_schema", True))
    max_size = int(spec.options.get("pool_max_size", 10))

    async def _check(conn: AsyncConnection[DictRow]) -> None:
        # Validate a pooled connection before checkout; raising discards it so
        # the pool reconnects instead of handing out a dropped socket.
        await conn.execute("SELECT 1")

    async with AsyncExitStack() as stack:
        pool = AsyncConnectionPool[AsyncConnection[DictRow]](
            conninfo=spec.uri,
            min_size=1,
            max_size=max_size,
            open=False,
            check=_check,
            kwargs={
                "autocommit": True,
                "prepare_threshold": 0,
                "row_factory": dict_row,
            },
        )
        await pool.open(wait=True)
        stack.push_async_callback(pool.close)
        inner = AsyncPostgresSaver(pool)
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

    # MongoDBSaver.from_conn_string is a *sync* context manager (backed by a
    # synchronous pymongo client) that exposes the async checkpoint methods
    # skeino uses; its sync __exit__ runs on the event loop at shutdown.
    setup_schema = bool(spec.options.get("setup_schema", True))
    db_name = mongo_db_from_uri(spec.uri)
    # Only override when the URI names a database (then the metadata store
    # derives the same name, so graph state and metadata share it). A pathless
    # URI keeps the saver's 'checkpointing_db' default — overriding it would
    # silently re-point existing deployments' checkpoints.
    saver_cm = (
        MongoDBSaver.from_conn_string(spec.uri)
        if db_name is None
        else MongoDBSaver.from_conn_string(spec.uri, db_name=db_name)
    )
    with saver_cm as saver:
        if setup_schema:
            for setup_name in ("asetup", "setup"):
                setup_fn = getattr(saver, setup_name, None)
                if setup_fn is not None:
                    result = setup_fn()
                    if inspect.isawaitable(result):
                        await result
                    break
        # MongoDBSaver merges config metadata (and run_id) into checkpoint
        # metadata itself, so no run-enriching wrapper is needed here.
        yield saver


@register_checkpointer("memory")
@asynccontextmanager
async def _build_memory(spec: CheckpointerSpec) -> AsyncIterator[BaseCheckpointSaver]:
    """Build an in-process MemorySaver. URI is ignored."""
    del spec
    yield MemorySaver()


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

    conn_string = normalize_sqlite_uri(spec.uri)
    setup_schema = bool(spec.options.get("setup_schema", True))
    async with AsyncExitStack() as stack:
        saver = await stack.enter_async_context(
            AsyncSqliteSaver.from_conn_string(conn_string)
        )
        if setup_schema and hasattr(saver, "setup"):
            result = saver.setup()
            if inspect.isawaitable(result):
                await result
        yield build_run_enriching_checkpointer(saver)


@register_checkpointer("redis")
@asynccontextmanager
async def _build_redis(spec: CheckpointerSpec) -> AsyncIterator[BaseCheckpointSaver]:
    """Build an async Redis checkpointer.

    Requires ``langgraph-checkpoint-redis``, which is not a managed skeino extra
    (it caps Python at <3.15) — install it yourself:
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
        yield build_run_enriching_checkpointer(saver)
