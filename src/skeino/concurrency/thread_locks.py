"""Per-thread asyncio locks + multitask-strategy enforcement.

Each thread has at most one in-flight run at any moment. The map is kept in
process — a clustered deployment would need a shared lock service, but the
skeino v1 target is the single-process langgraph-dev replacement.
"""

import asyncio

from fastapi import HTTPException, status

from skeino.schemas import MultitaskStrategy

_REJECT_STRATEGIES: frozenset[MultitaskStrategy] = frozenset(
    {"reject", "rollback", "interrupt"}
)


class ThreadLockManager:
    """Lookup-and-acquire helper for per-thread asyncio locks."""

    def __init__(self) -> None:
        """Initialise an empty lock map."""
        self._locks: dict[str, asyncio.Lock] = {}

    def get(self, thread_id: str) -> asyncio.Lock:
        """Return (creating on demand) the lock for ``thread_id``."""
        lock = self._locks.get(thread_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[thread_id] = lock
        return lock

    async def acquire(
        self,
        lock: asyncio.Lock,
        strategy: MultitaskStrategy,
        thread_id: str,
    ) -> None:
        """Apply ``strategy`` and acquire ``lock`` if permitted.

        Raises 409 Conflict for the reject/rollback/interrupt strategies when
        another run already holds the lock. ``enqueue`` always waits.
        """
        if strategy in _REJECT_STRATEGIES and lock.locked():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Thread {thread_id} already has an active run; "
                    f"multitask strategy {strategy!r} rejected."
                ),
            )
        await lock.acquire()
