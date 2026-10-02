"""Structural interface shared by the metadata store implementations.

Both :class:`skeino.persistence.MetadataStore` (Postgres-backed) and
:class:`skeino.persistence.InMemoryMetadataStore` satisfy this protocol. The
ops layer depends on :class:`MetadataStoreProtocol` rather than a concrete
class so alternative backends can be plugged in without touching it.

Every implementation returns the same row shapes, declared here as
:class:`ThreadRow` / :class:`RunRow` so the convention is a mypy-checked
contract rather than folklore. Value types are deliberately loose where the
database drivers differ (``status``, ``metadata``, …) — the contract is the
key set, not the inner types.
"""

from collections.abc import Collection, Sequence
from datetime import datetime
from typing import Any, Final, Protocol, TypedDict, runtime_checkable
from uuid import UUID

from skeino.schemas import (
    MultitaskStrategy,
    RunStatus,
    ThreadIfExists,
    ThreadSearchRequest,
    ThreadStatus,
    ThreadTtlConfig,
)
from skeino.schemas.common import JsonValue


class ThreadRow(TypedDict):
    """Uniform thread row shape every metadata store returns."""

    thread_id: UUID
    created_at: datetime
    updated_at: datetime
    state_updated_at: datetime | None
    metadata: dict[str, Any]
    config: dict[str, Any]
    status: Any  # ThreadStatus at runtime; loose so drivers' str passes
    ttl: dict[str, Any] | None


#: Run statuses that mean "not finished yet": the rows a heartbeat keeps alive
#: and an orphan sweep may fail. The SQL stores spell the same set as
#: ``status IN ('pending', 'running')``; the store contract pins them together.
IN_FLIGHT_RUN_STATUSES: Final[frozenset[RunStatus]] = frozenset({"pending", "running"})


class RunRow(TypedDict):
    """Uniform run row shape every metadata store returns."""

    run_id: UUID
    thread_id: UUID
    assistant_id: str
    created_at: datetime
    updated_at: datetime
    status: Any  # RunStatus at runtime; loose so drivers' str passes
    metadata: dict[str, Any]
    kwargs: dict[str, Any]
    multitask_strategy: Any  # MultitaskStrategy at runtime; loose
    error: str | None


@runtime_checkable
class MetadataStoreProtocol(Protocol):
    """Async CRUD surface for thread and run metadata."""

    async def setup(self) -> None:
        """Initialise the backing storage (create tables, etc.)."""
        ...

    async def fetch_thread_row(self, thread_id: str) -> ThreadRow | None:
        """Return the stored metadata row for a thread, or ``None``."""
        ...

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
        ...

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
        ...

    async def release_busy_thread(
        self,
        thread_id: str,
        status_value: ThreadStatus,
        *,
        mark_state_updated: bool = False,
    ) -> bool:
        """Move a ``busy`` thread with no run in flight to ``status_value``.

        Atomic with respect to run creation and other thread-status writes:
        the status is set only if, at the moment of the write, the thread is
        still ``busy`` and no ``pending``/``running`` run exists for it. A run
        created, or a status written, by another worker while the caller was
        deciding to release the thread therefore wins. Returns whether the
        thread was updated.
        """
        ...

    async def search_thread_rows(self, request: ThreadSearchRequest) -> list[ThreadRow]:
        """Return stored thread rows before graph-state enrichment."""
        ...

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a thread row and its run rows."""
        ...

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
        ...

    async def update_run_status(
        self,
        run_id: str,
        status_value: RunStatus,
        *,
        error: str | None = None,
    ) -> bool:
        """Update the persisted status of an in-flight run.

        Only a ``pending``/``running`` row is updated: a terminal status is
        final. Once another process's orphan sweep has failed a run, its late
        owner cannot flip it back to ``success`` or ``interrupted``. Returns
        whether a row was updated, so that owner can tell its write lost.
        """
        ...

    async def fetch_run_row(self, thread_id: str, run_id: str) -> RunRow | None:
        """Return a single run row for a thread, or ``None``."""
        ...

    async def list_run_rows(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[RunRow]:
        """List run rows for a thread."""
        ...

    async def delete_run(self, thread_id: str, run_id: str) -> None:
        """Delete a single run row, scoped to its thread."""
        ...

    async def touch_runs(self, run_ids: Sequence[str]) -> None:
        """Heartbeat: bump ``updated_at`` on the given runs that are in flight.

        Called periodically by the process executing them, so a ``pending`` /
        ``running`` row whose ``updated_at`` stops moving has lost its owner.
        Terminal rows are left untouched.
        """
        ...

    async def fail_stale_runs(
        self,
        *,
        stale_after_seconds: float,
        exclude_run_ids: Collection[str],
        error: str,
    ) -> list[RunRow]:
        """Mark orphaned in-flight runs ``error`` and return them.

        A run is orphaned when it is ``pending`` or ``running`` and its
        ``updated_at`` is more than ``stale_after_seconds`` old (its owner has
        stopped heartbeating), and it is not in ``exclude_run_ids`` (the
        caller's own live runs). Each row is claimed with a conditional update,
        so concurrent sweepers never both report the same run.

        Heartbeats and the staleness cutoff must come from one clock: Postgres
        uses the database's ``NOW()``; the SQLite and MongoDB stores use the
        writing process's clock, so workers sharing them need synchronised
        clocks.
        """
        ...
