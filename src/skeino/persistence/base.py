"""Structural interface shared by the metadata store implementations.

Both :class:`skeino.persistence.MetadataStore` (Postgres-backed) and
:class:`skeino.persistence.InMemoryMetadataStore` satisfy this protocol. The
ops layer depends on :class:`MetadataStoreProtocol` rather than a concrete
class so alternative backends can be plugged in without touching it.
"""

from typing import Any, Protocol, runtime_checkable

from skeino.schemas import (
    MultitaskStrategy,
    RunStatus,
    ThreadIfExists,
    ThreadSearchRequest,
    ThreadStatus,
    ThreadTtlConfig,
)
from skeino.schemas.common import JsonValue


@runtime_checkable
class MetadataStoreProtocol(Protocol):
    """Async CRUD surface for thread and run metadata."""

    async def setup(self) -> None:
        """Initialise the backing storage (create tables, etc.)."""
        ...

    async def fetch_thread_row(self, thread_id: str) -> dict[str, Any] | None:
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
    ) -> dict[str, Any]:
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

    async def search_thread_rows(
        self, request: ThreadSearchRequest
    ) -> list[dict[str, Any]]:
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
    ) -> dict[str, Any]:
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

    async def fetch_run_row(self, thread_id: str, run_id: str) -> dict[str, Any] | None:
        """Return a single run row for a thread, or ``None``."""
        ...

    async def list_run_rows(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: RunStatus | None,
    ) -> list[dict[str, Any]]:
        """List run rows for a thread."""
        ...
