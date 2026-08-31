"""Persistent thread + run metadata stored alongside LangGraph checkpoints.

The LangGraph checkpointer stores graph values (messages, evidence, etc.) but
not the higher-level API concepts like ``status``, ``ttl``, or the relationship
between a run and its parent thread. ``MetadataStore`` owns those two tables
(``app_threads``, ``app_runs``) and exposes the CRUD surface the runtime needs.

Postgres (psycopg) is an optional dependency (``skeino[postgres]``); it is
imported lazily so importing this module never requires it. Operations run over
a shared :class:`~psycopg_pool.AsyncConnectionPool`, opened on first use and
closed by :meth:`MetadataStore.aclose` — see that method and
``_connection`` for why a connection *per operation* was untenable.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from fastapi import HTTPException, status

from skeino.persistence.base import RunRow, ThreadRow
from skeino.schemas import (
    JsonValue,
    MultitaskStrategy,
    RunStatus,
    ThreadIfExists,
    ThreadSearchRequest,
    ThreadStatus,
    ThreadTtlConfig,
)

THREAD_STATUS_IDLE: Final[ThreadStatus] = "idle"
RUN_STATUS_PENDING: Final[RunStatus] = "pending"
DEFAULT_SORT_BY: Final[str] = "updated_at"
DEFAULT_SORT_ORDER: Final[str] = "desc"
THREAD_SORT_FIELDS: Final[frozenset[str]] = frozenset(
    {"thread_id", "status", "created_at", "updated_at", "state_updated_at"}
)

_CREATE_THREADS_TABLE_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS app_threads (
    thread_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state_updated_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    ttl JSONB
)
"""

_CREATE_RUNS_TABLE_SQL: Final[str] = """
CREATE TABLE IF NOT EXISTS app_runs (
    run_id UUID PRIMARY KEY,
    thread_id UUID NOT NULL REFERENCES app_threads(thread_id) ON DELETE CASCADE,
    assistant_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    kwargs JSONB NOT NULL DEFAULT '{}'::jsonb,
    multitask_strategy TEXT NOT NULL,
    error TEXT
)
"""

# Indexes are built CONCURRENTLY, on a connection of their own. A plain CREATE
# INDEX holds a lock that blocks writes for the whole table scan, so adding one
# to an existing large table would stall traffic on every instance's startup —
# precisely the deployments these indexes exist to speed up. CONCURRENTLY
# cannot run inside a transaction, hence the separate autocommit connection in
# ``_create_indexes``. Each entry is (name, create, drop); the drop is spelled
# out rather than interpolated so no SQL here is ever built from a variable.
_INDEXES: Final[tuple[tuple[str, str, str], ...]] = (
    (
        "idx_app_runs_thread_created",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_runs_thread_created "
        "ON app_runs (thread_id, created_at DESC)",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_app_runs_thread_created",
    ),
    (
        # Thread search sorts by updated_at DESC by default (DEFAULT_SORT_BY);
        # without this the paginated listing sorts the whole table per page.
        "idx_app_threads_updated_at",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_app_threads_updated_at "
        "ON app_threads (updated_at DESC)",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_app_threads_updated_at",
    ),
)

# A concurrent build that fails leaves the index behind marked invalid, and
# CREATE INDEX IF NOT EXISTS then skips it forever — the table would stay
# silently unindexed. Detect that leftover so it can be dropped and rebuilt.
_INVALID_INDEX_SQL: Final[str] = """
SELECT 1
FROM pg_class c
JOIN pg_index i ON i.indexrelid = c.oid
WHERE c.relname = %s AND NOT i.indisvalid
"""


def _utcnow() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(UTC)


_POSTGRES_EXTRA_HINT: Final[str] = (
    "The 'postgres' metadata store requires the skeino[postgres] extra "
    "(pip install 'skeino[postgres]')."
)


def _pg() -> tuple[Any, Any]:
    """Lazily import psycopg (optional dependency: skeino[postgres])."""
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(_POSTGRES_EXTRA_HINT) from exc
    return psycopg, dict_row


def _pg_pool() -> Any:
    """Lazily import psycopg_pool (optional dependency: skeino[postgres])."""
    try:
        from psycopg_pool import AsyncConnectionPool
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(_POSTGRES_EXTRA_HINT) from exc
    return AsyncConnectionPool


def _to_jsonb(payload: dict[str, JsonValue] | None) -> Any:
    """Wrap a JSON-serializable dictionary for psycopg JSONB parameters."""
    if payload is None:
        return None
    try:
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(_POSTGRES_EXTRA_HINT) from exc
    return Jsonb(payload)


class MetadataStore:
    """Persist thread and run metadata alongside LangGraph checkpoints."""

    def __init__(self, postgres_uri: str, *, pool_max_size: int = 10) -> None:
        """Store the PostgreSQL connection string used for metadata operations."""
        self._postgres_uri = postgres_uri
        self._pool_max_size = pool_max_size
        self._pool: Any | None = None

    async def _ensure_pool(self) -> Any:
        """Return the shared connection pool, opening it on first use.

        Every operation used to open its own ``AsyncConnection``, which meant a
        TCP connect, a TLS handshake and a SCRAM exchange per *query*. Against a
        managed Postgres in another region that is several round trips of pure
        latency before any row moves, and the cost multiplies with the number of
        queries a request makes — ``GET``-style listings that read one row per
        result paid it once per row.

        ``check`` validates a pooled connection before checkout so one dropped
        by an idle-timeout or a recycling pooler is replaced rather than handed
        out closed. ``prepare_threshold=None`` disables client-side prepared
        statements, which keeps the store correct behind a transaction-mode
        pooler (pgbouncer, Supabase) where a later query may land on a different
        server-side session. It must be ``None`` and not ``0``: psycopg prepares
        a statement once its execution count reaches the threshold, so ``0``
        prepares on the *first* execution — the opposite of disabling.
        Mirrors the checkpointer pool in ``checkpointer``.
        """
        if self._pool is not None:
            return self._pool
        _, dict_row = _pg()
        async_connection_pool = _pg_pool()

        async def _check(conn: Any) -> None:
            await conn.execute("SELECT 1")

        pool = async_connection_pool(
            conninfo=self._postgres_uri,
            min_size=1,
            max_size=self._pool_max_size,
            open=False,
            check=_check,
            kwargs={"prepare_threshold": None, "row_factory": dict_row},
        )
        await pool.open(wait=True)
        self._pool = pool
        return pool

    @asynccontextmanager
    async def _connection(self) -> AsyncIterator[Any]:
        """Check a connection out of the pool for the duration of the block.

        ``pool.connection()`` commits on a clean exit and rolls back on an
        exception, so callers keep their explicit ``commit()`` only where they
        need the write visible before the block ends.
        """
        pool = await self._ensure_pool()
        async with pool.connection() as conn:
            yield conn

    async def aclose(self) -> None:
        """Close the connection pool (called on app shutdown)."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def setup(self) -> None:
        """Create the metadata tables and indexes if they do not already exist."""
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(_CREATE_THREADS_TABLE_SQL)
                await cursor.execute(_CREATE_RUNS_TABLE_SQL)
            await conn.commit()
        await self._create_indexes()

    async def _create_indexes(self) -> None:
        """Build the search indexes without locking writes out of the tables.

        ``CREATE INDEX CONCURRENTLY`` cannot run inside a transaction, so this
        opens its own autocommit connection instead of borrowing a pooled one
        (pool connections are transactional, and the pool's liveness ``check``
        may already have opened a transaction on them).

        An earlier build that failed leaves an invalid index that
        ``IF NOT EXISTS`` would skip forever, so any such leftover is dropped
        and rebuilt rather than silently tolerated.
        """
        psycopg, _ = _pg()
        conn = await psycopg.AsyncConnection.connect(
            self._postgres_uri, autocommit=True
        )
        try:
            for name, create_sql, drop_sql in _INDEXES:
                async with conn.cursor() as cursor:
                    await cursor.execute(_INVALID_INDEX_SQL, (name,))
                    if await cursor.fetchone() is not None:
                        await cursor.execute(drop_sql)
                    await cursor.execute(create_sql)
        finally:
            await conn.close()

    @staticmethod
    async def _select_thread_row(conn: Any, thread_id: str) -> ThreadRow | None:
        """Read one thread row on an already-checked-out connection."""
        async with conn.cursor() as cursor:
            # The SELECT/RETURNING column lists in this module exactly
            # mirror ThreadRow/RunRow — that is what makes the dict_row →
            # TypedDict annotations below sound.
            await cursor.execute(
                """
                SELECT thread_id, created_at, updated_at, state_updated_at,
                       metadata, config, status, ttl
                FROM app_threads
                WHERE thread_id = %s
                """,
                (thread_id,),
            )
            row: ThreadRow | None = await cursor.fetchone()
            return row

    async def fetch_thread_row(self, thread_id: str) -> ThreadRow | None:
        """Return the stored metadata row for a thread."""
        async with self._connection() as conn:
            return await self._select_thread_row(conn, thread_id)

    async def create_thread(
        self,
        thread_id: str,
        *,
        metadata: dict[str, JsonValue],
        config: dict[str, JsonValue],
        ttl: ThreadTtlConfig | None,
        if_exists: ThreadIfExists,
    ) -> ThreadRow:
        """Insert a thread row and return the stored record."""
        psycopg, _ = _pg()
        ttl_payload = self._build_ttl_payload(ttl)
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                try:
                    await cursor.execute(
                        """
                        INSERT INTO app_threads (
                            thread_id, metadata, config, status, ttl
                        ) VALUES (%s, %s, %s, %s, %s)
                        RETURNING thread_id, created_at, updated_at, state_updated_at,
                                  metadata, config, status, ttl
                        """,
                        (
                            thread_id,
                            _to_jsonb(metadata),
                            _to_jsonb(config),
                            THREAD_STATUS_IDLE,
                            _to_jsonb(ttl_payload),
                        ),
                    )
                except psycopg.errors.UniqueViolation as exc:
                    await conn.rollback()
                    if if_exists == "do_nothing":
                        # Re-read on *this* connection. Calling the public
                        # fetch_thread_row here would check a second connection
                        # out of the pool while still holding this one, which
                        # deadlocks once the pool is saturated.
                        existing_row = await self._select_thread_row(conn, thread_id)
                        if existing_row is None:
                            raise HTTPException(
                                status_code=status.HTTP_409_CONFLICT,
                                detail=f"Thread {thread_id} insert conflicted but "
                                "the row could not be re-read (concurrent delete?).",
                            ) from exc
                        return existing_row
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"Thread {thread_id} already exists.",
                    ) from exc
                created_row: ThreadRow | None = await cursor.fetchone()
            await conn.commit()
        if created_row is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create thread {thread_id}.",
            )
        return created_row

    async def update_thread(
        self,
        thread_id: str,
        *,
        status_value: ThreadStatus | None = None,
        config: dict[str, JsonValue] | None = None,
        metadata: dict[str, JsonValue] | None = None,
        mark_state_updated: bool = False,
    ) -> None:
        """Update mutable metadata for a thread."""
        assignments: list[str] = ["updated_at = NOW()"]
        values: list[Any] = []
        if status_value is not None:
            assignments.append("status = %s")
            values.append(status_value)
        if config is not None:
            assignments.append("config = %s")
            values.append(_to_jsonb(config))
        if metadata is not None:
            assignments.append("metadata = %s")
            values.append(_to_jsonb(metadata))
        if mark_state_updated:
            assignments.append("state_updated_at = NOW()")

        if len(assignments) == 1:
            return

        values.append(thread_id)
        # nosec B608: `assignments` holds only hardcoded "column = %s"/NOW()
        # fragments built above; every user value is bound via %s parameters.
        query = f"""
            UPDATE app_threads
            SET {", ".join(assignments)}
            WHERE thread_id = %s
        """  # nosec B608
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, values)
            await conn.commit()

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a thread row and its run rows."""
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    "DELETE FROM app_runs WHERE thread_id = %s", (thread_id,)
                )
                await cursor.execute(
                    "DELETE FROM app_threads WHERE thread_id = %s", (thread_id,)
                )
            await conn.commit()

    async def search_thread_rows(
        self,
        request: ThreadSearchRequest,
    ) -> list[ThreadRow]:
        """Return stored thread rows before graph-state enrichment."""
        conditions: list[str] = []
        values: list[Any] = []
        if request.ids:
            conditions.append("thread_id = ANY(%s)")
            values.append([str(thread_id) for thread_id in request.ids])
        if request.status is not None:
            conditions.append("status = %s")
            values.append(request.status)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sort_by = request.sort_by or DEFAULT_SORT_BY
        if sort_by not in THREAD_SORT_FIELDS:
            sort_by = DEFAULT_SORT_BY
        sort_order = request.sort_order or DEFAULT_SORT_ORDER
        if sort_order not in {"asc", "desc"}:
            sort_order = DEFAULT_SORT_ORDER

        # nosec B608: `where_clause` is composed of hardcoded "column = %s"
        # conditions, and `sort_by`/`sort_order` are whitelisted against
        # THREAD_SORT_FIELDS and {"asc","desc"}; user values are bound via %s.
        query = f"""
            SELECT thread_id, created_at, updated_at, state_updated_at,
                   metadata, config, status, ttl
            FROM app_threads
            {where_clause}
            ORDER BY {sort_by} {sort_order.upper()}
            LIMIT %s
            OFFSET %s
        """  # nosec B608
        values.extend([request.limit, request.offset])
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, values)
                rows = await cursor.fetchall()
        return list(rows)

    async def create_run(
        self,
        run_id: str,
        thread_id: str,
        assistant_id: str,
        metadata: dict[str, JsonValue],
        kwargs: dict[str, JsonValue],
        multitask_strategy: MultitaskStrategy,
    ) -> RunRow:
        """Insert a run row and return it."""
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT INTO app_runs (
                        run_id, thread_id, assistant_id, status,
                        metadata, kwargs, multitask_strategy
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING run_id, thread_id, assistant_id, created_at, updated_at,
                              status, metadata, kwargs, multitask_strategy, error
                    """,
                    (
                        run_id,
                        thread_id,
                        assistant_id,
                        RUN_STATUS_PENDING,
                        _to_jsonb(metadata),
                        _to_jsonb(kwargs),
                        multitask_strategy,
                    ),
                )
                run_row: RunRow | None = await cursor.fetchone()
            await conn.commit()
        if run_row is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create run {run_id}.",
            )
        return run_row

    async def update_run_status(
        self,
        run_id: str,
        status_value: RunStatus,
        *,
        error: str | None = None,
    ) -> None:
        """Update the persisted run status."""
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    UPDATE app_runs
                    SET status = %s, updated_at = NOW(), error = %s
                    WHERE run_id = %s
                    """,
                    (status_value, error, run_id),
                )
            await conn.commit()

    async def fetch_run_row(self, thread_id: str, run_id: str) -> RunRow | None:
        """Return a single run row for a thread."""
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT run_id, thread_id, assistant_id, created_at, updated_at,
                           status, metadata, kwargs, multitask_strategy, error
                    FROM app_runs
                    WHERE thread_id = %s AND run_id = %s
                    """,
                    (thread_id, run_id),
                )
                row: RunRow | None = await cursor.fetchone()
                return row

    async def list_run_rows(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[RunRow]:
        """List run rows for a thread."""
        conditions: list[str] = ["thread_id = %s"]
        values: list[Any] = [thread_id]
        if status_value is not None:
            conditions.append("status = %s")
            values.append(status_value)

        # nosec B608: `conditions` holds only hardcoded "column = %s" fragments
        # built above; every user value is bound via %s parameters.
        query = f"""
            SELECT run_id, thread_id, assistant_id, created_at, updated_at,
                   status, metadata, kwargs, multitask_strategy, error
            FROM app_runs
            WHERE {" AND ".join(conditions)}
            ORDER BY created_at DESC
            LIMIT %s
            OFFSET %s
        """  # nosec B608
        values.extend([limit, offset])
        async with self._connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, values)
                rows = await cursor.fetchall()
        return list(rows)

    def _build_ttl_payload(
        self, ttl: ThreadTtlConfig | None
    ) -> dict[str, JsonValue] | None:
        """Return the stored TTL payload."""
        if ttl is None or ttl.ttl is None:
            return None
        expires_at = _utcnow() + timedelta(minutes=ttl.ttl)
        return {
            "strategy": ttl.strategy,
            "ttl_minutes": ttl.ttl,
            "expires_at": expires_at.isoformat(),
        }
