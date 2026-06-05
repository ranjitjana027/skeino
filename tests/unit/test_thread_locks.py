"""Tests for the per-thread asyncio lock manager."""

import asyncio

import pytest
from fastapi import HTTPException
from skeino.concurrency import ThreadLockManager


def test_get_returns_same_lock_for_same_thread() -> None:
    mgr = ThreadLockManager()
    assert mgr.get("t1") is mgr.get("t1")
    assert mgr.get("t1") is not mgr.get("t2")


@pytest.mark.asyncio
async def test_acquire_enqueue_waits() -> None:
    mgr = ThreadLockManager()
    lock = mgr.get("t1")
    await mgr.acquire(lock, "enqueue", "t1")

    async def second() -> None:
        await mgr.acquire(lock, "enqueue", "t1")
        lock.release()

    task = asyncio.create_task(second())
    await asyncio.sleep(0.05)
    assert not task.done()
    lock.release()
    await task


@pytest.mark.asyncio
async def test_acquire_reject_raises_on_busy_lock() -> None:
    mgr = ThreadLockManager()
    lock = mgr.get("t1")
    await mgr.acquire(lock, "reject", "t1")
    with pytest.raises(HTTPException) as exc:
        await mgr.acquire(lock, "reject", "t1")
    assert exc.value.status_code == 409
    lock.release()
