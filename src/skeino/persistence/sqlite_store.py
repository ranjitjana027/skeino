"""SQLite-backed MetadataStore — durable thread/run metadata without Postgres.

Mirrors :class:`skeino.persistence.MetadataStore` /
:class:`skeino.persistence.InMemoryMetadataStore` semantics on top of
``aiosqlite``. JSON columns are stored as TEXT and timestamps as ISO-8601
strings, then converted back to ``dict`` / ``datetime`` (and ids to ``UUID``) on
read so the returned row shapes match the other stores exactly.

``aiosqlite`` is an optional dependency (``skeino[sqlite]``); it is imported
lazily in :meth:`SqliteMetadataStore.setup` so importing this module never
requires it. A single connection is held for the store's lifetime (so an
in-memory database survives across operations) and access is serialised with an
``asyncio.Lock`` — correct for skeino's single-process model. File-backed
databases are opened in WAL mode with a busy timeout so sharing the file with
the SQLite checkpointer doesn't hit ``database is locked`` under concurrent
writes.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import HTTPException, status

from skeino.persistence.base import RunRow, ThreadRow
from skeino.persistence.uri import normalize_sqlite_uri
from skeino.schemas import (
    JsonValue,
    MultitaskStrategy,
    RunStatus,
    ThreadIfExists,
    ThreadSearchRequest,
    ThreadStatus,
    ThreadTtlConfig,
)

_THREAD_SORT_FIELDS: frozenset[str] = frozenset(
    {"thread_id", "status", "created_at", "updated_at", "state_updated_at"}
)
_DEFAULT_SORT_BY = "updated_at"

_CREATE_THREADS_SQL = """
CREATE TABLE IF NOT EXISTS app_threads (
    thread_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    state_updated_at TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    config TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    ttl TEXT
)
"""
_CREATE_RUNS_SQL = """
CREATE TABLE IF NOT EXISTS app_runs (
    run_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES app_threads(thread_id) ON DELETE CASCADE,
    assistant_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    kwargs TEXT NOT NULL DEFAULT '{}',
    multitask_strategy TEXT NOT NULL,
    error TEXT
)
"""
_CREATE_RUNS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_app_runs_thread_created
ON app_runs (thread_id, created_at DESC)
"""

_THREAD_COLUMNS = (
    "thread_id, created_at, updated_at, state_updated_at, metadata, config, status, ttl"
)
_RUN_COLUMNS = (
    "run_id, thread_id, assistant_id, created_at, updated_at, "
    "status, metadata, kwargs, multitask_strategy, error"
)

# Wait out a concurrent writer (e.g. the SQLite checkpointer sharing the
# file) instead of failing with "database is locked". Deliberately above the
# driver's 5 s default so the setting is observable.
_BUSY_TIMEOUT_MS = 10_000


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _to_dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value is not None else None


class SqliteMetadataStore:
    """SQLite-backed thread + run metadata store satisfying MetadataStoreProtocol."""

    def __init__(self, path: str) -> None:
        """Store the SQLite path/URI (a file path, ``:memory:``, or ``sqlite://``)."""
        self._path = normalize_sqlite_uri(path)
        self._conn: Any = None
        self._lock = asyncio.Lock()

    async def setup(self) -> None:
        """Open the connection (lazily importing aiosqlite) and create tables."""
        try:
            import aiosqlite  # optional dependency: skeino[sqlite]
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "The 'sqlite' metadata store requires the skeino[sqlite] extra "
                "(pip install 'skeino[sqlite]')."
            ) from exc

        self._conn = await aiosqlite.connect(self._path)
        # Busy timeout first: the WAL conversion below takes an exclusive lock
        # and must itself wait out a concurrent checkpointer connection.
        await self._conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
        # WAL lets readers and a writer coexist on a shared file; a harmless
        # no-op on ":memory:" (journal_mode stays "memory").
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.execute(_CREATE_THREADS_SQL)
        await self._conn.execute(_CREATE_RUNS_SQL)
        await self._conn.execute(_CREATE_RUNS_INDEX_SQL)
        await self._conn.commit()

    async def aclose(self) -> None:
        """Close the underlying connection (called on app shutdown)."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------ threads

    @staticmethod
    def _thread_row(row: Any) -> ThreadRow:
        return {
            "thread_id": UUID(row[0]),
            "created_at": datetime.fromisoformat(row[1]),
            "updated_at": datetime.fromisoformat(row[2]),
            "state_updated_at": _to_dt(row[3]),
            "metadata": json.loads(row[4]),
            "config": json.loads(row[5]),
            "status": row[6],
            "ttl": json.loads(row[7]) if row[7] is not None else None,
        }

    async def fetch_thread_row(self, thread_id: str) -> ThreadRow | None:
        """Return the stored row for ``thread_id`` (or None)."""
        async with self._lock:
            cursor = await self._conn.execute(
                "SELECT thread_id, created_at, updated_at, state_updated_at, "
                "metadata, config, status, ttl FROM app_threads WHERE thread_id = ?",
                (thread_id,),
            )
            row = await cursor.fetchone()
        return self._thread_row(row) if row is not None else None

    async def create_thread(
        self,
        thread_id: str,
        *,
        metadata: dict[str, JsonValue],
        config: dict[str, JsonValue],
        ttl: ThreadTtlConfig | None,
        if_exists: ThreadIfExists,
    ) -> ThreadRow:
        """Insert a thread row and return it."""
        now = _utcnow()
        ttl_payload = self._ttl_payload(ttl, now)
        async with self._lock:
            try:
                await self._conn.execute(
                    "INSERT INTO app_threads "
                    "(thread_id, created_at, updated_at, metadata, config, status, ttl)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        thread_id,
                        now.isoformat(),
                        now.isoformat(),
                        json.dumps(metadata),
                        json.dumps(config),
                        "idle",
                        json.dumps(ttl_payload) if ttl_payload is not None else None,
                    ),
                )
                await self._conn.commit()
            except sqlite3.IntegrityError as exc:
                await self._conn.rollback()
                if if_exists == "do_nothing":
                    existing = await self._fetch_thread_locked(thread_id)
                    if existing is not None:
                        return existing
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"Thread {thread_id} insert conflicted but the row "
                        "could not be re-read (concurrent delete?).",
                    ) from exc
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Thread {thread_id} already exists.",
                ) from exc
        return {
            "thread_id": UUID(thread_id),
            "created_at": now,
            "updated_at": now,
            "state_updated_at": None,
            "metadata": dict(metadata),
            "config": dict(config),
            "status": "idle",
            "ttl": ttl_payload,
        }

    async def _fetch_thread_locked(self, thread_id: str) -> ThreadRow | None:
        """Fetch a thread row assuming the lock is already held."""
        cursor = await self._conn.execute(
            "SELECT thread_id, created_at, updated_at, state_updated_at, "
            "metadata, config, status, ttl FROM app_threads WHERE thread_id = ?",
            (thread_id,),
        )
        row = await cursor.fetchone()
        return self._thread_row(row) if row is not None else None

    async def update_thread(
        self,
        thread_id: str,
        *,
        status_value: ThreadStatus | None = None,
        config: dict[str, JsonValue] | None = None,
        metadata: dict[str, JsonValue] | None = None,
        mark_state_updated: bool = False,
    ) -> None:
        """Update mutable thread fields."""
        assignments = ["updated_at = ?"]
        values: list[Any] = [_utcnow().isoformat()]
        if status_value is not None:
            assignments.append("status = ?")
            values.append(status_value)
        if config is not None:
            assignments.append("config = ?")
            values.append(json.dumps(config))
        if metadata is not None:
            assignments.append("metadata = ?")
            values.append(json.dumps(metadata))
        if mark_state_updated:
            assignments.append("state_updated_at = ?")
            values.append(_utcnow().isoformat())
        if len(assignments) == 1:
            return
        values.append(thread_id)
        # `assignments` holds only hardcoded "column = ?" fragments; user values
        # are bound via ? parameters.
        query = f"UPDATE app_threads SET {', '.join(assignments)} WHERE thread_id = ?"  # nosec B608
        async with self._lock:
            await self._conn.execute(query, values)
            await self._conn.commit()

    async def search_thread_rows(self, request: ThreadSearchRequest) -> list[ThreadRow]:
        """Return stored thread rows (filtered by ids/status, sorted, paginated)."""
        conditions: list[str] = []
        values: list[Any] = []
        if request.ids:
            placeholders = ", ".join("?" for _ in request.ids)
            conditions.append(f"thread_id IN ({placeholders})")
            values.extend(str(item) for item in request.ids)
        if request.status is not None:
            conditions.append("status = ?")
            values.append(request.status)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sort_by = request.sort_by or _DEFAULT_SORT_BY
        if sort_by not in _THREAD_SORT_FIELDS:
            sort_by = _DEFAULT_SORT_BY
        order = "ASC" if request.sort_order == "asc" else "DESC"
        values.extend([request.limit, request.offset])
        # `where` is composed of hardcoded "column = ?"/"IN (?)" fragments;
        # sort_by is whitelisted against _THREAD_SORT_FIELDS and order is a
        # literal ASC/DESC; every user value is bound via ? parameters.
        query = f"SELECT {_THREAD_COLUMNS} FROM app_threads {where} ORDER BY {sort_by} {order} LIMIT ? OFFSET ?"  # nosec B608
        async with self._lock:
            cursor = await self._conn.execute(query, values)
            rows = await cursor.fetchall()
        return [self._thread_row(row) for row in rows]

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a thread and its run rows."""
        async with self._lock:
            await self._conn.execute(
                "DELETE FROM app_runs WHERE thread_id = ?", (thread_id,)
            )
            await self._conn.execute(
                "DELETE FROM app_threads WHERE thread_id = ?", (thread_id,)
            )
            await self._conn.commit()

    # --------------------------------------------------------------------- runs

    @staticmethod
    def _run_row(row: Any) -> RunRow:
        return {
            "run_id": UUID(row[0]),
            "thread_id": UUID(row[1]),
            "assistant_id": row[2],
            "created_at": datetime.fromisoformat(row[3]),
            "updated_at": datetime.fromisoformat(row[4]),
            "status": row[5],
            "metadata": json.loads(row[6]),
            "kwargs": json.loads(row[7]),
            "multitask_strategy": row[8],
            "error": row[9],
        }

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
        now = _utcnow()
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO app_runs "
                "(run_id, thread_id, assistant_id, created_at, updated_at, "
                "status, metadata, kwargs, multitask_strategy)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    thread_id,
                    assistant_id,
                    now.isoformat(),
                    now.isoformat(),
                    "pending",
                    json.dumps(metadata),
                    json.dumps(kwargs),
                    multitask_strategy,
                ),
            )
            await self._conn.commit()
        return {
            "run_id": UUID(run_id),
            "thread_id": UUID(thread_id),
            "assistant_id": assistant_id,
            "created_at": now,
            "updated_at": now,
            "status": "pending",
            "metadata": dict(metadata),
            "kwargs": dict(kwargs),
            "multitask_strategy": multitask_strategy,
            "error": None,
        }

    async def update_run_status(
        self,
        run_id: str,
        status_value: RunStatus,
        *,
        error: str | None = None,
    ) -> None:
        """Update a run's status field."""
        async with self._lock:
            await self._conn.execute(
                "UPDATE app_runs SET status = ?, updated_at = ?, error = ? "
                "WHERE run_id = ?",
                (status_value, _utcnow().isoformat(), error, run_id),
            )
            await self._conn.commit()

    async def fetch_run_row(self, thread_id: str, run_id: str) -> RunRow | None:
        """Return a run row scoped to ``thread_id``."""
        async with self._lock:
            cursor = await self._conn.execute(
                f"SELECT {_RUN_COLUMNS} FROM app_runs "  # nosec B608 - static columns
                "WHERE thread_id = ? AND run_id = ?",
                (thread_id, run_id),
            )
            row = await cursor.fetchone()
        return self._run_row(row) if row is not None else None

    async def list_run_rows(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[RunRow]:
        """List runs for a thread sorted newest-first."""
        conditions = ["thread_id = ?"]
        values: list[Any] = [thread_id]
        if status_value is not None:
            conditions.append("status = ?")
            values.append(status_value)
        values.extend([limit, offset])
        # conditions are hardcoded "column = ?" fragments; user values are
        # bound via ? parameters.
        where = " AND ".join(conditions)
        query = f"SELECT {_RUN_COLUMNS} FROM app_runs WHERE {where} ORDER BY created_at DESC LIMIT ? OFFSET ?"  # nosec B608
        async with self._lock:
            cursor = await self._conn.execute(query, values)
            rows = await cursor.fetchall()
        return [self._run_row(row) for row in rows]

    @staticmethod
    def _ttl_payload(
        ttl: ThreadTtlConfig | None, now: datetime
    ) -> dict[str, JsonValue] | None:
        if ttl is None or ttl.ttl is None:
            return None
        return {
            "strategy": ttl.strategy,
            "ttl_minutes": ttl.ttl,
            "expires_at": (now + timedelta(minutes=ttl.ttl)).isoformat(),
        }
