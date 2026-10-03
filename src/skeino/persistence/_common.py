"""Helpers shared by the metadata store implementations."""

from datetime import UTC, datetime, timedelta
from typing import Final
from uuid import UUID

from fastapi import HTTPException, status

from skeino.persistence.base import RunRow, ThreadRow
from skeino.schemas import (
    JsonValue,
    MultitaskStrategy,
    ThreadSearchRequest,
    ThreadTtlConfig,
)

DEFAULT_SORT_BY: Final[str] = "updated_at"
THREAD_SORT_FIELDS: Final[frozenset[str]] = frozenset(
    {"thread_id", "status", "created_at", "updated_at", "state_updated_at"}
)


def utcnow() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(UTC)


def missing_extra(component: str, extra: str) -> RuntimeError:
    """Build the error raised when an optional dependency is not installed."""
    return RuntimeError(
        f"The '{extra}' {component} requires the skeino[{extra}] extra "
        f"(pip install 'skeino[{extra}]')."
    )


def resolve_sort_by(request: ThreadSearchRequest) -> str:
    """Return the whitelisted thread sort column for ``request``."""
    sort_by = request.sort_by or DEFAULT_SORT_BY
    return sort_by if sort_by in THREAD_SORT_FIELDS else DEFAULT_SORT_BY


def ttl_payload(
    ttl: ThreadTtlConfig | None, now: datetime
) -> dict[str, JsonValue] | None:
    """Return the stored TTL payload for a new thread, or ``None``."""
    if ttl is None or ttl.ttl is None:
        return None
    return {
        "strategy": ttl.strategy,
        "ttl_minutes": ttl.ttl,
        "expires_at": (now + timedelta(minutes=ttl.ttl)).isoformat(),
    }


def thread_exists_error(thread_id: str) -> HTTPException:
    """409 for creating a thread whose id is taken (``if_exists="raise"``)."""
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"Thread {thread_id} already exists.",
    )


def thread_reread_error(thread_id: str) -> HTTPException:
    """409 for a ``do_nothing`` conflict whose existing row vanished."""
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"Thread {thread_id} insert conflicted but the row "
        "could not be re-read (concurrent delete?).",
    )


def new_thread_row(
    thread_id: str,
    now: datetime,
    *,
    metadata: dict[str, JsonValue],
    config: dict[str, JsonValue],
    ttl: dict[str, JsonValue] | None,
) -> ThreadRow:
    """Return the row of a freshly created thread."""
    return {
        "thread_id": UUID(thread_id),
        "created_at": now,
        "updated_at": now,
        "state_updated_at": None,
        "metadata": dict(metadata),
        "config": dict(config),
        "status": "idle",
        "ttl": ttl,
    }


def new_run_row(
    run_id: str,
    thread_id: str,
    now: datetime,
    *,
    assistant_id: str,
    metadata: dict[str, JsonValue],
    kwargs: dict[str, JsonValue],
    multitask_strategy: MultitaskStrategy,
) -> RunRow:
    """Return the row of a freshly created (``pending``) run."""
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
