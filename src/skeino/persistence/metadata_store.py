"""Persistent thread + run metadata stored alongside LangGraph checkpoints.

The LangGraph checkpointer stores graph values (messages, evidence, etc.) but
not the higher-level API concepts like ``status``, ``ttl``, or the relationship
between a run and its parent thread. ``MetadataStore`` owns those two tables
(``app_threads``, ``app_runs``) and exposes the CRUD surface the runtime needs.

Postgres (psycopg) is an optional dependency (``skeino[postgres]``); it is
imported lazily so importing this module never requires it. Connection
management is intentionally simple: each operation opens a fresh
``psycopg.AsyncConnection``.
"""

from datetime import UTC, datetime, timedelta
from typing import Any, Final

from fastapi import HTTPException, status

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

_CREATE_RUNS_THREAD_INDEX_SQL: Final[str] = """
CREATE INDEX IF NOT EXISTS idx_app_runs_thread_created
ON app_runs (thread_id, created_at DESC)
"""


def _utcnow() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(UTC)


def _pg() -> tuple[Any, Any]:
    """Lazily import psycopg (optional dependency: skeino[postgres])."""
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "The 'postgres' metadata store requires the skeino[postgres] extra "
            "(pip install 'skeino[postgres]')."
        ) from exc
    return psycopg, dict_row


def _to_jsonb(payload: dict[str, JsonValue] | None) -> Any:
    """Wrap a JSON-serializable dictionary for psycopg JSONB parameters."""
    if payload is None:
        return None
    from psycopg.types.json import Jsonb

    return Jsonb(payload)


class MetadataStore:
    """Persist thread and run metadata alongside LangGraph checkpoints."""

    def __init__(self, postgres_uri: str) -> None:
        """Store the PostgreSQL connection string used for metadata operations."""
        self._postgres_uri = postgres_uri

    async def setup(self) -> None:
        """Create the metadata tables if they do not already exist."""
        psycopg, _ = _pg()
        async with await psycopg.AsyncConnection.connect(self._postgres_uri) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(_CREATE_THREADS_TABLE_SQL)
                await cursor.execute(_CREATE_RUNS_TABLE_SQL)
                await cursor.execute(_CREATE_RUNS_THREAD_INDEX_SQL)
            await conn.commit()

    async def fetch_thread_row(self, thread_id: str) -> dict[str, Any] | None:
        """Return the stored metadata row for a thread."""
        psycopg, dict_row = _pg()
        async with await psycopg.AsyncConnection.connect(
            self._postgres_uri, row_factory=dict_row
        ) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT thread_id, created_at, updated_at, state_updated_at,
                           metadata, config, status, ttl
                    FROM app_threads
                    WHERE thread_id = %s
                    """,
                    (thread_id,),
                )
                row: dict[str, Any] | None = await cursor.fetchone()
                return row

    async def create_thread(
        self,
        thread_id: str,
        *,
        metadata: dict[str, JsonValue],
        config: dict[str, JsonValue],
        ttl: ThreadTtlConfig | None,
        if_exists: ThreadIfExists,
    ) -> dict[str, Any]:
        """Insert a thread row and return the stored record."""
        psycopg, dict_row = _pg()
        ttl_payload = self._build_ttl_payload(ttl)
        async with await psycopg.AsyncConnection.connect(
            self._postgres_uri, row_factory=dict_row
        ) as conn:
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
                        existing_row = await self.fetch_thread_row(thread_id)
                        if existing_row is None:
                            raise HTTPException(
                                status_code=status.HTTP_409_CONFLICT,
                                detail=f"Thread {thread_id} already exists.",
                            ) from exc
                        return existing_row
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"Thread {thread_id} already exists.",
                    ) from exc
                created_row: dict[str, Any] | None = await cursor.fetchone()
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
        psycopg, _ = _pg()
        # nosec B608: `assignments` holds only hardcoded "column = %s"/NOW()
        # fragments built above; every user value is bound via %s parameters.
        query = f"""
            UPDATE app_threads
            SET {", ".join(assignments)}
            WHERE thread_id = %s
        """  # nosec B608
        async with await psycopg.AsyncConnection.connect(self._postgres_uri) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, values)
            await conn.commit()

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a thread row and its run rows."""
        psycopg, _ = _pg()
        async with await psycopg.AsyncConnection.connect(self._postgres_uri) as conn:
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
    ) -> list[dict[str, Any]]:
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
        psycopg, dict_row = _pg()
        async with await psycopg.AsyncConnection.connect(
            self._postgres_uri, row_factory=dict_row
        ) as conn:
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
    ) -> dict[str, Any]:
        """Insert a run row and return it."""
        psycopg, dict_row = _pg()
        async with await psycopg.AsyncConnection.connect(
            self._postgres_uri, row_factory=dict_row
        ) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    INSERT INTO app_runs (
                        run_id, thread_id, assistant_id, status,
                        metadata, kwargs, multitask_strategy
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING run_id, thread_id, assistant_id, created_at, updated_at,
                              status, metadata, kwargs, multitask_strategy
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
                run_row: dict[str, Any] | None = await cursor.fetchone()
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
        psycopg, _ = _pg()
        async with await psycopg.AsyncConnection.connect(self._postgres_uri) as conn:
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

    async def fetch_run_row(self, thread_id: str, run_id: str) -> dict[str, Any] | None:
        """Return a single run row for a thread."""
        psycopg, dict_row = _pg()
        async with await psycopg.AsyncConnection.connect(
            self._postgres_uri, row_factory=dict_row
        ) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT run_id, thread_id, assistant_id, created_at, updated_at,
                           status, metadata, kwargs, multitask_strategy
                    FROM app_runs
                    WHERE thread_id = %s AND run_id = %s
                    """,
                    (thread_id, run_id),
                )
                row: dict[str, Any] | None = await cursor.fetchone()
                return row

    async def list_run_rows(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[dict[str, Any]]:
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
                   status, metadata, kwargs, multitask_strategy
            FROM app_runs
            WHERE {" AND ".join(conditions)}
            ORDER BY created_at DESC
            LIMIT %s
            OFFSET %s
        """  # nosec B608
        values.extend([limit, offset])
        psycopg, dict_row = _pg()
        async with await psycopg.AsyncConnection.connect(
            self._postgres_uri, row_factory=dict_row
        ) as conn:
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
