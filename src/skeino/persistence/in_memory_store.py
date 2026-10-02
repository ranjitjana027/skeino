"""In-process MetadataStore implementation.

Provides the same async surface as :class:`skeino.persistence.MetadataStore`
without touching Postgres. Useful for tests, ``langgraph dev``-style local
runs, and any deployment where durability is not required.
"""

from collections.abc import Collection, Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

from fastapi import HTTPException, status

from skeino.persistence.base import IN_FLIGHT_RUN_STATUSES, RunRow, ThreadRow
from skeino.schemas import (
    JsonValue,
    MultitaskStrategy,
    RunStatus,
    ThreadIfExists,
    ThreadSearchRequest,
    ThreadStatus,
    ThreadTtlConfig,
)


def _utcnow() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(UTC)


class InMemoryMetadataStore:
    """Dict-backed thread + run metadata store."""

    def __init__(self) -> None:
        """Initialise empty thread and run maps."""
        self._threads: dict[str, ThreadRow] = {}
        self._runs: dict[str, RunRow] = {}

    async def setup(self) -> None:
        """No-op; the dicts are already ready."""
        return None

    async def fetch_thread_row(self, thread_id: str) -> ThreadRow | None:
        """Return the stored row for ``thread_id`` (or None)."""
        return self._threads.get(thread_id)

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
        if thread_id in self._threads:
            if if_exists == "do_nothing":
                return self._threads[thread_id]
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Thread {thread_id} already exists.",
            )
        now = _utcnow()
        ttl_payload: dict[str, JsonValue] | None = None
        if ttl is not None and ttl.ttl is not None:
            ttl_payload = {
                "strategy": ttl.strategy,
                "ttl_minutes": ttl.ttl,
                "expires_at": (now + timedelta(minutes=ttl.ttl)).isoformat(),
            }
        row: ThreadRow = {
            "thread_id": UUID(thread_id),
            "created_at": now,
            "updated_at": now,
            "state_updated_at": None,
            "metadata": dict(metadata),
            "config": dict(config),
            "status": "idle",
            "ttl": ttl_payload,
        }
        self._threads[thread_id] = row
        return row

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
        row = self._threads.get(thread_id)
        if row is None:
            return
        row["updated_at"] = _utcnow()
        if status_value is not None:
            row["status"] = status_value
        if config is not None:
            row["config"] = dict(config)
        if metadata is not None:
            row["metadata"] = dict(metadata)
        if mark_state_updated:
            row["state_updated_at"] = _utcnow()

    async def release_busy_thread(
        self,
        thread_id: str,
        status_value: ThreadStatus,
        *,
        mark_state_updated: bool = False,
    ) -> bool:
        """Set a busy thread's status if no run is in flight on it.

        Atomic: nothing here awaits, so no other coroutine interleaves.
        """
        row = self._threads.get(thread_id)
        if row is None or row["status"] != "busy":
            return False
        if any(
            str(run["thread_id"]) == thread_id
            and run["status"] in IN_FLIGHT_RUN_STATUSES
            for run in self._runs.values()
        ):
            return False
        now = _utcnow()
        row["updated_at"] = now
        row["status"] = status_value
        if mark_state_updated:
            row["state_updated_at"] = now
        return True

    async def search_thread_rows(self, request: ThreadSearchRequest) -> list[ThreadRow]:
        """List thread rows respecting basic filter / pagination flags."""
        rows = list(self._threads.values())
        if request.ids:
            allowed = {str(item) for item in request.ids}
            rows = [r for r in rows if str(r["thread_id"]) in allowed]
        if request.status is not None:
            rows = [r for r in rows if r["status"] == request.status]
        rows.sort(key=lambda r: r["updated_at"], reverse=(request.sort_order != "asc"))
        return rows[request.offset : request.offset + request.limit]

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a thread and its run rows."""
        self._threads.pop(thread_id, None)
        self._runs = {
            run_id: row
            for run_id, row in self._runs.items()
            if str(row["thread_id"]) != thread_id
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
        row: RunRow = {
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
        self._runs[run_id] = row
        return row

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
        row = self._runs.get(run_id)
        if row is None or row["status"] not in IN_FLIGHT_RUN_STATUSES:
            return False
        row["status"] = status_value
        row["updated_at"] = _utcnow()
        # Always assign (clearing with None) — same semantics as the SQL/Mongo
        # stores, which unconditionally write the error column on update.
        row["error"] = error
        return True

    async def fetch_run_row(self, thread_id: str, run_id: str) -> RunRow | None:
        """Return a run row scoped to ``thread_id``."""
        row = self._runs.get(run_id)
        if row is None or str(row["thread_id"]) != thread_id:
            return None
        return row

    async def list_run_rows(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[RunRow]:
        """List runs for a thread sorted newest-first."""
        rows = [r for r in self._runs.values() if str(r["thread_id"]) == thread_id]
        if status_value is not None:
            rows = [r for r in rows if r["status"] == status_value]
        rows.sort(key=lambda r: r["created_at"], reverse=True)
        return rows[offset : offset + limit]

    async def delete_run(self, thread_id: str, run_id: str) -> None:
        """Delete a run row scoped to ``thread_id``."""
        row = self._runs.get(run_id)
        if row is not None and str(row["thread_id"]) == thread_id:
            del self._runs[run_id]

    async def touch_runs(self, run_ids: Sequence[str]) -> None:
        """Bump ``updated_at`` on the given in-flight runs (heartbeat)."""
        now = _utcnow()
        for run_id in run_ids:
            row = self._runs.get(run_id)
            if row is not None and row["status"] in IN_FLIGHT_RUN_STATUSES:
                row["updated_at"] = now

    async def fail_stale_runs(
        self,
        *,
        stale_after_seconds: float,
        exclude_run_ids: Collection[str],
        error: str,
    ) -> list[RunRow]:
        """Mark in-flight runs not heartbeated within the window ``error``."""
        now = _utcnow()
        cutoff = now - timedelta(seconds=stale_after_seconds)
        excluded = set(exclude_run_ids)
        failed: list[RunRow] = []
        for run_id, row in self._runs.items():
            if (
                row["status"] in IN_FLIGHT_RUN_STATUSES
                and row["updated_at"] < cutoff
                and run_id not in excluded
            ):
                row["status"] = "error"
                row["error"] = error
                row["updated_at"] = now
                failed.append(dict(row))  # type: ignore[arg-type]
        return failed
