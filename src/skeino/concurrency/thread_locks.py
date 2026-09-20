"""Per-thread asyncio locks.

Each thread has at most one in-flight run at any moment; the lock enforces that
serialisation while a background run executes. The map is kept in process — a
clustered deployment would need a shared lock service, but the skeino v1 target
is the single-process langgraph-dev replacement.

Multitask-strategy enforcement (reject/enqueue/interrupt/rollback) lives in
``RunOps`` admission, not here: this is now a plain per-thread lock map. The
``enqueue`` strategy is realised by the background run task simply waiting on
``await lock.acquire()`` — queued runs serialise on the lock (mutual exclusion;
one run executes at a time).
"""

import asyncio


class ThreadLockManager:
    """Lookup helper for per-thread asyncio locks."""

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
