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

from datetime import datetime
from typing import Any, Protocol, TypedDict, runtime_checkable
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
    ) -> None:
        """Update the persisted run status."""
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
