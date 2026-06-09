"""MongoDB-backed MetadataStore — durable thread/run metadata on MongoDB.

Mirrors the other metadata stores' row shapes (UUID ids, ``datetime``
timestamps, dict JSON fields) on top of ``motor`` (async MongoDB). ``motor`` is
an optional dependency (``skeino[mongodb]``), imported lazily in
:meth:`MongoMetadataStore.setup` so importing this module never requires it.

Thread and run documents use the id as ``_id`` (so duplicate inserts raise a
``DuplicateKeyError``); runs are deleted with their thread.

The database defaults to the one named in the ``mongodb://…/<db>`` URI path —
matching the checkpointer builder, so graph state and metadata share the
operator's chosen database — falling back to ``skeino`` for pathless URIs.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import HTTPException, status

from skeino.persistence.base import RunRow, ThreadRow
from skeino.persistence.uri import mongo_db_from_uri
from skeino.schemas import (
    JsonValue,
    MultitaskStrategy,
    RunStatus,
    ThreadIfExists,
    ThreadSearchRequest,
    ThreadStatus,
    ThreadTtlConfig,
)

_DEFAULT_DB_NAME = "skeino"
_THREAD_SORT_FIELDS: frozenset[str] = frozenset(
    {"thread_id", "status", "created_at", "updated_at", "state_updated_at"}
)
_DEFAULT_SORT_BY = "updated_at"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class MongoMetadataStore:
    """MongoDB-backed thread + run metadata store (MetadataStoreProtocol)."""

    def __init__(self, uri: str, *, db_name: str | None = None) -> None:
        """Store the URI; ``db_name`` defaults to the URI's path, else "skeino"."""
        self._uri = uri
        self._db_name = db_name or mongo_db_from_uri(uri) or _DEFAULT_DB_NAME
        self._client: Any = None
        self._threads: Any = None
        self._runs: Any = None

    async def setup(self) -> None:
        """Open the motor client (lazily) and ensure indexes."""
        try:
            import motor.motor_asyncio  # optional dependency: skeino[mongodb]
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "The 'mongodb' metadata store requires the skeino[mongodb] extra "
                "(pip install 'skeino[mongodb]')."
            ) from exc

        self._client = motor.motor_asyncio.AsyncIOMotorClient(self._uri)
        db = self._client[self._db_name]
        self._threads = db["app_threads"]
        self._runs = db["app_runs"]
        await self._threads.create_index([("status", 1), ("updated_at", -1)])
        await self._runs.create_index([("thread_id", 1), ("created_at", -1)])

    async def aclose(self) -> None:
        """Close the motor client (called on app shutdown)."""
        if self._client is not None:
            self._client.close()
            self._client = None

    # ------------------------------------------------------------------ threads

    @staticmethod
    def _thread_row(doc: dict[str, Any]) -> ThreadRow:
        return {
            "thread_id": UUID(doc["thread_id"]),
            "created_at": doc["created_at"],
            "updated_at": doc["updated_at"],
            "state_updated_at": doc.get("state_updated_at"),
            "metadata": doc.get("metadata", {}),
            "config": doc.get("config", {}),
            "status": doc["status"],
            "ttl": doc.get("ttl"),
        }

    async def fetch_thread_row(self, thread_id: str) -> ThreadRow | None:
        """Return the stored row for ``thread_id`` (or None)."""
        doc = await self._threads.find_one({"_id": thread_id})
        return self._thread_row(doc) if doc is not None else None

    async def create_thread(
        self,
        thread_id: str,
        *,
        metadata: dict[str, JsonValue],
        config: dict[str, JsonValue],
        ttl: ThreadTtlConfig | None,
        if_exists: ThreadIfExists,
    ) -> ThreadRow:
        """Insert a thread document and return its row."""
        from pymongo.errors import DuplicateKeyError

        now = _utcnow()
        ttl_payload = self._ttl_payload(ttl, now)
        doc = {
            "_id": thread_id,
            "thread_id": thread_id,
            "created_at": now,
            "updated_at": now,
            "state_updated_at": None,
            "metadata": dict(metadata),
            "config": dict(config),
            "status": "idle",
            "ttl": ttl_payload,
        }
        try:
            await self._threads.insert_one(doc)
        except DuplicateKeyError as exc:
            if if_exists == "do_nothing":
                existing = await self.fetch_thread_row(thread_id)
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
        return self._thread_row(doc)

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
        updates: dict[str, Any] = {"updated_at": _utcnow()}
        if status_value is not None:
            updates["status"] = status_value
        if config is not None:
            updates["config"] = dict(config)
        if metadata is not None:
            updates["metadata"] = dict(metadata)
        if mark_state_updated:
            updates["state_updated_at"] = _utcnow()
        if len(updates) == 1:
            return
        await self._threads.update_one({"_id": thread_id}, {"$set": updates})

    async def search_thread_rows(self, request: ThreadSearchRequest) -> list[ThreadRow]:
        """Return stored thread rows (filtered by ids/status, sorted, paginated)."""
        query: dict[str, Any] = {}
        if request.ids:
            query["_id"] = {"$in": [str(item) for item in request.ids]}
        if request.status is not None:
            query["status"] = request.status
        sort_by = request.sort_by or _DEFAULT_SORT_BY
        if sort_by not in _THREAD_SORT_FIELDS:
            sort_by = _DEFAULT_SORT_BY
        direction = 1 if request.sort_order == "asc" else -1
        cursor = (
            self._threads.find(query)
            .sort(sort_by, direction)
            .skip(request.offset)
            .limit(request.limit)
        )
        return [self._thread_row(doc) async for doc in cursor]

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a thread and its run documents."""
        await self._runs.delete_many({"thread_id": thread_id})
        await self._threads.delete_one({"_id": thread_id})

    # --------------------------------------------------------------------- runs

    @staticmethod
    def _run_row(doc: dict[str, Any]) -> RunRow:
        return {
            "run_id": UUID(doc["run_id"]),
            "thread_id": UUID(doc["thread_id"]),
            "assistant_id": doc["assistant_id"],
            "created_at": doc["created_at"],
            "updated_at": doc["updated_at"],
            "status": doc["status"],
            "metadata": doc.get("metadata", {}),
            "kwargs": doc.get("kwargs", {}),
            "multitask_strategy": doc["multitask_strategy"],
            "error": doc.get("error"),
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
        """Insert a run document and return its row."""
        now = _utcnow()
        doc = {
            "_id": run_id,
            "run_id": run_id,
            "thread_id": thread_id,
            "assistant_id": assistant_id,
            "created_at": now,
            "updated_at": now,
            "status": "pending",
            "metadata": dict(metadata),
            "kwargs": dict(kwargs),
            "multitask_strategy": multitask_strategy,
            "error": None,
        }
        await self._runs.insert_one(doc)
        return self._run_row(doc)

    async def update_run_status(
        self,
        run_id: str,
        status_value: RunStatus,
        *,
        error: str | None = None,
    ) -> None:
        """Update a run's status field."""
        await self._runs.update_one(
            {"_id": run_id},
            {"$set": {"status": status_value, "updated_at": _utcnow(), "error": error}},
        )

    async def fetch_run_row(self, thread_id: str, run_id: str) -> RunRow | None:
        """Return a run row scoped to ``thread_id``."""
        doc = await self._runs.find_one({"_id": run_id, "thread_id": thread_id})
        return self._run_row(doc) if doc is not None else None

    async def list_run_rows(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[RunRow]:
        """List runs for a thread sorted newest-first."""
        query: dict[str, Any] = {"thread_id": thread_id}
        if status_value is not None:
            query["status"] = status_value
        cursor = self._runs.find(query).sort("created_at", -1).skip(offset).limit(limit)
        return [self._run_row(doc) async for doc in cursor]

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
