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

import logging
from collections.abc import Collection, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import HTTPException, status

from skeino.persistence.base import IN_FLIGHT_RUN_STATUSES, RunRow, ThreadRow
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

logger = logging.getLogger(__name__)


# How long a run creation's reservation on its thread holds off
# ``release_busy_thread``. Creating a run takes milliseconds; the bound only
# matters for a process that died mid-creation, whose reservation would
# otherwise block the thread's release forever.
_CREATION_RESERVATION_TTL = timedelta(minutes=1)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime) -> datetime:
    """BSON datetimes come back naive (UTC); make them comparable."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


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
        # Orphan sweep: in-flight runs by heartbeat age.
        await self._runs.create_index([("status", 1), ("updated_at", 1)])

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
        change: dict[str, Any] = {"$set": updates}
        if status_value is not None:
            # Lets ``release_busy_thread`` detect a status written meanwhile.
            change["$inc"] = {"version": 1}
        await self._threads.update_one({"_id": thread_id}, change)

    async def release_busy_thread(
        self,
        thread_id: str,
        status_value: ThreadStatus,
        *,
        mark_state_updated: bool = False,
    ) -> bool:
        """Set a busy thread's status if no run is in flight on it.

        Threads and runs are separate collections, so the check cannot be one
        atomic write without a multi-document transaction (which needs a
        replica set). Instead the write is a compare-and-set on the thread's
        ``version``, which every status write bumps. :meth:`create_run`
        reserves the thread (bumping the version) before inserting its run and
        clears the reservation after. So a run being created is either
        reserved when the thread is read (and the release backs off), already
        inserted when runs are checked, or reserved after the read (and the
        compare-and-set fails). A reservation older than
        ``_CREATION_RESERVATION_TTL`` belongs to a process that died
        mid-creation; it is ignored, and cleared by a successful release.
        """
        doc = await self._threads.find_one(
            {"_id": thread_id, "status": "busy"}, {"version": 1, "creating": 1}
        )
        if doc is None:
            return False
        creating: dict[str, datetime] = doc.get("creating") or {}
        expired = _utcnow() - _CREATION_RESERVATION_TTL
        if any(_as_utc(reserved) > expired for reserved in creating.values()):
            return False
        in_flight = await self._runs.find_one(
            {
                "thread_id": thread_id,
                "status": {"$in": sorted(IN_FLIGHT_RUN_STATUSES)},
            },
            {"_id": 1},
        )
        if in_flight is not None:
            return False
        now = _utcnow()
        updates: dict[str, Any] = {"status": status_value, "updated_at": now}
        if mark_state_updated:
            updates["state_updated_at"] = now
        change: dict[str, Any] = {"$set": updates, "$inc": {"version": 1}}
        if creating:  # every one of them expired
            change["$unset"] = {f"creating.{run_id}": "" for run_id in creating}
        result = await self._threads.update_one(
            {
                "_id": thread_id,
                "status": "busy",
                # ``None`` also matches a thread written before the counter.
                "version": doc.get("version"),
            },
            change,
        )
        return bool(result.matched_count)

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
        reservation = f"creating.{run_id}"
        # Reserve the thread for the insert (see ``release_busy_thread``).
        await self._threads.update_one(
            {"_id": thread_id}, {"$set": {reservation: now}, "$inc": {"version": 1}}
        )
        try:
            await self._runs.insert_one(doc)
        except BaseException:
            await self._drop_reservation(thread_id, reservation)
            raise
        try:
            await self._threads.update_one(
                {"_id": thread_id}, {"$unset": {reservation: ""}}
            )
        except BaseException:
            # A run whose creation failed has no owner: undo the insert rather
            # than leave a ``pending`` row looking in flight until the orphan
            # timeout. The reservation expires on its own.
            try:
                await self._runs.delete_one({"_id": run_id})
            except Exception as cleanup_exc:
                logger.error(
                    "Failed to remove run %s after its creation failed",
                    run_id,
                    exc_info=cleanup_exc,
                )
            raise
        return self._run_row(doc)

    async def _drop_reservation(self, thread_id: str, reservation: str) -> None:
        """Best-effort: an undropped reservation expires on its own."""
        try:
            await self._threads.update_one(
                {"_id": thread_id}, {"$unset": {reservation: ""}}
            )
        except Exception as exc:
            logger.error(
                "Failed to drop run-creation reservation %s on thread %s",
                reservation,
                thread_id,
                exc_info=exc,
            )

    async def update_run_status(
        self,
        run_id: str,
        status_value: RunStatus,
        *,
        error: str | None = None,
    ) -> bool:
        """Update an in-flight run's status; return whether a row was updated.

        Terminal rows are left as they are.
        """
        result = await self._runs.update_one(
            {"_id": run_id, "status": {"$in": sorted(IN_FLIGHT_RUN_STATUSES)}},
            {"$set": {"status": status_value, "updated_at": _utcnow(), "error": error}},
        )
        return bool(result.matched_count)

    async def touch_runs(self, run_ids: Sequence[str]) -> None:
        """Bump ``updated_at`` on the given in-flight runs (heartbeat)."""
        if not run_ids:
            return
        await self._runs.update_many(
            {
                "_id": {"$in": list(run_ids)},
                "status": {"$in": sorted(IN_FLIGHT_RUN_STATUSES)},
            },
            {"$set": {"updated_at": _utcnow()}},
        )

    async def fail_stale_runs(
        self,
        *,
        stale_after_seconds: float,
        exclude_run_ids: Collection[str],
        error: str,
    ) -> list[RunRow]:
        """Mark in-flight runs not heartbeated within the window ``error``.

        Each candidate is claimed with ``find_one_and_update`` on the same
        stale filter, so a heartbeat (or another sweeper) that lands between
        the scan and the claim wins. Each claim commits on its own, so if the
        scan fails after some claims succeeded those runs are still returned
        (and the failure logged) rather than failed without a report.

        Staleness compares ``updated_at`` values written by each worker's own
        clock, so workers sharing the database need synchronised clocks (NTP);
        skew eats into ``orphaned_run_timeout_seconds - run_heartbeat_seconds``.
        """
        from pymongo import ReturnDocument  # optional dependency: skeino[mongodb]

        now = _utcnow()
        stale = {
            "status": {"$in": sorted(IN_FLIGHT_RUN_STATUSES)},
            "updated_at": {"$lt": now - timedelta(seconds=stale_after_seconds)},
            "_id": {"$nin": list(exclude_run_ids)},
        }
        failed: list[RunRow] = []
        try:
            async for doc in self._runs.find(stale, {"_id": 1}):
                claimed = await self._runs.find_one_and_update(
                    {**stale, "_id": doc["_id"]},
                    {"$set": {"status": "error", "error": error, "updated_at": now}},
                    return_document=ReturnDocument.AFTER,
                )
                if claimed is not None:
                    failed.append(self._run_row(claimed))
        except Exception as exc:
            if not failed:
                raise
            logger.error(
                "Orphan sweep stopped after claiming %d run(s); the rest are "
                "retried on the next sweep",
                len(failed),
                exc_info=exc,
            )
        return failed

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

    async def delete_run(self, thread_id: str, run_id: str) -> None:
        """Delete a single run document scoped to its thread."""
        await self._runs.delete_one({"_id": run_id, "thread_id": thread_id})

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
