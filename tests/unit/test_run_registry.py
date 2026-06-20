"""Tests for the background run task registry."""

import asyncio

import pytest

from skeino.concurrency import BackgroundRunRegistry


async def test_spawn_tracks_then_forgets_on_completion() -> None:
    reg = BackgroundRunRegistry()

    async def work() -> int:
        return 7

    task = reg.spawn("t1", "r1", work())
    assert "r1" in reg.active_runs("t1")
    assert await task == 7
    await asyncio.sleep(0)  # let the done-callback run
    assert reg.active_runs("t1") == set()
    assert reg.get("r1") is None


async def test_cancel_returns_false_for_unknown_run() -> None:
    reg = BackgroundRunRegistry()
    assert await reg.cancel("nope", wait=True) is False


async def test_cancel_waits_for_task_to_unwind() -> None:
    reg = BackgroundRunRegistry()
    gate = asyncio.Event()
    cleaned = asyncio.Event()

    async def work() -> None:
        try:
            await gate.wait()
        except asyncio.CancelledError:
            cleaned.set()
            raise

    task = reg.spawn("t1", "r1", work())
    await asyncio.sleep(0)  # let the task park on the gate
    assert await reg.cancel("r1", wait=True) is True
    assert cleaned.is_set()
    assert task.cancelled()


async def test_external_registration_counts_as_active_but_not_cancellable() -> None:
    reg = BackgroundRunRegistry()
    reg.register_external("t1", "stream1")
    assert "stream1" in reg.active_runs("t1")
    # No task backing it, so it cannot be cancelled.
    assert await reg.cancel("stream1", wait=False) is False
    reg.unregister_external("t1", "stream1")
    assert reg.active_runs("t1") == set()


async def test_shutdown_cancels_all_tracked_tasks() -> None:
    reg = BackgroundRunRegistry()
    gate = asyncio.Event()

    async def work() -> None:
        await gate.wait()

    tasks = [reg.spawn("t1", f"r{i}", work()) for i in range(3)]
    await asyncio.sleep(0)
    await reg.shutdown()
    assert all(t.cancelled() for t in tasks)


@pytest.mark.asyncio
async def test_admission_serialises_per_thread() -> None:
    reg = BackgroundRunRegistry()
    order: list[str] = []

    async def critical(tag: str) -> None:
        async with reg.admission("t1"):
            order.append(f"{tag}-enter")
            await asyncio.sleep(0.01)
            order.append(f"{tag}-exit")

    await asyncio.gather(critical("a"), critical("b"))
    # The second entrant cannot interleave inside the first's critical section.
    assert order in (
        ["a-enter", "a-exit", "b-enter", "b-exit"],
        ["b-enter", "b-exit", "a-enter", "a-exit"],
    )
