"""In-process registry of background run tasks.

Each run executes inside an :class:`asyncio.Task` tracked here so that the
server can return immediately (background create), await a task (wait / join),
or cancel it (cancel + multitask interrupt/rollback). The map is process-local
— the same single-process scope assumption as :class:`ThreadLockManager`; a
clustered deployment would need a shared task service.

This is a pure concurrency primitive: it tracks task lifecycle and serialises
admission per thread, but knows nothing about persistence or the multitask
*policy* (reject/enqueue/interrupt/rollback). The policy lives in ``RunOps``,
which decides — under the admission lock exposed here — what to cancel or delete
before spawning a new run.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Coroutine


class BackgroundRunRegistry:
    """Track and control background run tasks for the current process."""

    def __init__(self) -> None:
        """Initialise empty task / active-run / admission-lock maps."""
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._active_by_thread: dict[str, set[str]] = {}
        self._admission_locks: dict[str, asyncio.Lock] = {}

    @asynccontextmanager
    async def admission(self, thread_id: str) -> AsyncIterator[None]:
        """Hold the per-thread admission lock for the duration of the block.

        Serialises the check-strategy-then-spawn step so concurrent creates on
        one thread cannot race a ``reject`` check or double-cancel an active run.
        """
        lock = self._admission_locks.get(thread_id)
        if lock is None:
            lock = asyncio.Lock()
            self._admission_locks[thread_id] = lock
        async with lock:
            yield

    def active_runs(self, thread_id: str) -> set[str]:
        """Return the set of non-terminal run ids tracked for ``thread_id``."""
        return set(self._active_by_thread.get(thread_id, ()))

    def all_active(self) -> list[tuple[str, str]]:
        """Return ``(thread_id, run_id)`` for every currently-active run."""
        return [
            (thread_id, run_id)
            for thread_id, run_ids in self._active_by_thread.items()
            for run_id in run_ids
        ]

    def get(self, run_id: str) -> asyncio.Task[Any] | None:
        """Return the live task for ``run_id`` (or ``None`` if not running)."""
        return self._tasks.get(run_id)

    def spawn(
        self, thread_id: str, run_id: str, coro: Coroutine[Any, Any, Any]
    ) -> asyncio.Task[Any]:
        """Schedule ``coro`` as a tracked background task and return it.

        The run is registered as active *before* the task runs, so a concurrent
        admission decision under the same thread's lock sees it immediately.
        """
        task = asyncio.create_task(coro)
        self._tasks[run_id] = task
        self._active_by_thread.setdefault(thread_id, set()).add(run_id)
        task.add_done_callback(lambda _t: self._forget(thread_id, run_id))
        return task

    def register_external(self, thread_id: str, run_id: str) -> None:
        """Mark a run active without a tracked task (e.g. a live streaming run).

        Such a run counts toward admission (so ``reject`` sees it) but has no
        cancellable task — :meth:`cancel` returns ``False`` for it.
        """
        self._active_by_thread.setdefault(thread_id, set()).add(run_id)

    def unregister_external(self, thread_id: str, run_id: str) -> None:
        """Drop a run registered with :meth:`register_external`."""
        self._forget(thread_id, run_id)

    def _forget(self, thread_id: str, run_id: str) -> None:
        """Drop a finished run from the task / active maps."""
        self._tasks.pop(run_id, None)
        active = self._active_by_thread.get(thread_id)
        if active is not None:
            active.discard(run_id)
            if not active:
                del self._active_by_thread[thread_id]

    async def cancel(self, run_id: str, *, wait: bool) -> bool:
        """Cancel a live run task. Return whether one existed.

        With ``wait`` the call blocks until the task has fully unwound (so its
        ``interrupted`` state is persisted and its lock released before we
        return). ``asyncio.wait`` is used so a cancelled task does not re-raise
        into this coroutine.
        """
        task = self._tasks.get(run_id)
        if task is None:
            return False
        task.cancel()
        if wait:
            await asyncio.wait({task})
        return True

    async def shutdown(self) -> None:
        """Cancel and await every tracked task (runtime shutdown)."""
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(set(tasks))
