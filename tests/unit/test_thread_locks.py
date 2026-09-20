"""Tests for the per-thread asyncio lock manager."""

import asyncio

import pytest

from skeino.concurrency import ThreadLockManager


def test_get_returns_same_lock_for_same_thread() -> None:
    mgr = ThreadLockManager()
    assert mgr.get("t1") is mgr.get("t1")
    assert mgr.get("t1") is not mgr.get("t2")


@pytest.mark.asyncio
async def test_lock_serialises_runs_fifo() -> None:
    # The lock map is a plain per-thread asyncio.Lock; ``enqueue`` semantics are
    # realised by waiting on it (a second acquirer blocks until release).
    mgr = ThreadLockManager()
    lock = mgr.get("t1")
    await lock.acquire()

    async def second() -> None:
        await lock.acquire()
        lock.release()

    task = asyncio.create_task(second())
    await asyncio.sleep(0.05)
    assert not task.done()
    lock.release()
    await task
