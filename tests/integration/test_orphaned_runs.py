"""Runs orphaned by a dead process are failed, not left ``running`` forever.

A run's task lives in the process that started it. When that process dies
without a graceful shutdown, its rows stay ``pending``/``running`` and pollers
(and ``enqueue`` runs) wait forever. Each process heartbeats the runs it owns;
a run whose heartbeat stopped is failed with a reason and its thread released.

Two apps over one SQLite file stand in for two workers (or a restart) sharing
a durable metadata store. A row written straight into the store with an old
``updated_at`` stands in for a run whose process died mid-flight.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI

from skeino import SkeinoSettings, create_app
from skeino.schemas import RunCreateRequest
from tests.conftest import FakeGraph

HEARTBEAT = 0.05
TIMEOUT = 0.3


def _app(db: Path, graph: FakeGraph, **settings: Any) -> FastAPI:
    return create_app(
        graphs={"agent": lambda _ckpt: graph},
        settings=SkeinoSettings(
            default_assistant_id="agent",
            checkpointer_scheme="sqlite",
            checkpointer_uri=str(db),
            **{
                "run_heartbeat_seconds": HEARTBEAT,
                "orphaned_run_timeout_seconds": TIMEOUT,
                **settings,
            },
        ),
    )


@asynccontextmanager
async def _running(app: FastAPI) -> AsyncIterator[Any]:
    async with app.router.lifespan_context(app):
        yield app.state.skeino.run_ops


async def _crashed_run(run_ops: Any, *, age_seconds: float) -> tuple[str, str]:
    """A thread ``busy`` with a ``running`` run nobody is executing."""
    thread_id, run_id = str(uuid4()), str(uuid4())
    store = run_ops._metadata_store
    await store.create_thread(
        thread_id, metadata={}, config={}, ttl=None, if_exists="raise"
    )
    await store.create_run(
        run_id, thread_id, "agent", metadata={}, kwargs={}, multitask_strategy="enqueue"
    )
    await store.update_run_status(run_id, "running")
    await store.update_thread(thread_id, status_value="busy")
    old = (datetime.now(UTC) - timedelta(seconds=age_seconds)).isoformat()
    async with store._lock:
        await store._conn.execute(
            "UPDATE app_runs SET updated_at = ? WHERE run_id = ?", (old, run_id)
        )
        await store._conn.commit()
    return thread_id, run_id


async def _status(run_ops: Any, thread_id: str, run_id: str) -> tuple[str, str | None]:
    row = await run_ops._metadata_store.fetch_run_row(thread_id, run_id)
    return str(row["status"]), row["error"]


async def test_restart_fails_runs_left_running_by_the_previous_process(
    tmp_path: Path,
) -> None:
    db = tmp_path / "skeino.db"
    # "Previous process": leaves a run running, then dies (no shutdown).
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        thread_id, run_id = await _crashed_run(ops, age_seconds=3600)

    # Restart: the startup pass fails it before the app serves anything.
    async with _running(_app(db, FakeGraph())) as ops:
        run_status, error = await _status(ops, thread_id, run_id)
        assert run_status == "error"
        assert error is not None and "orphaned" in error
        thread = await ops._metadata_store.fetch_thread_row(thread_id)
        assert thread["status"] == "error"  # no longer stuck "busy"


async def test_sweep_spares_live_runs_of_another_worker_and_fails_dead_ones(
    tmp_path: Path,
) -> None:
    db = tmp_path / "skeino.db"
    graph_a = FakeGraph()
    graph_a.invoke_gate = asyncio.Event()
    async with (
        _running(_app(db, graph_a)) as worker_a,
        _running(_app(db, FakeGraph())) as worker_b,
    ):
        # Worker A executes a long run: its own heartbeat keeps it alive.
        live = await worker_a.create_run(
            str(uuid4()),
            RunCreateRequest(
                assistant_id="agent", input={"messages": []}, if_not_exists="create"
            ),
        )
        await graph_a.invoke_started.wait()
        # A third worker died mid-run a moment ago (fresh, not yet stale).
        dead_thread, dead_run = await _crashed_run(worker_b, age_seconds=0)

        # Several timeout windows pass; both workers sweep the shared store.
        await asyncio.sleep(TIMEOUT * 3)

        live_status, _ = await _status(worker_b, str(live.thread_id), str(live.run_id))
        assert live_status == "running"  # B never kills A's heartbeating run
        dead_status, _ = await _status(worker_b, dead_thread, dead_run)
        assert dead_status == "error"

        graph_a.invoke_gate.set()
        await worker_a.join_run(str(live.thread_id), str(live.run_id))
        assert (await _status(worker_a, str(live.thread_id), str(live.run_id)))[
            0
        ] == "success"


async def test_thread_with_other_in_flight_work_stays_busy(tmp_path: Path) -> None:
    # Failing one orphan must not flip a thread whose other run is still live.
    db = tmp_path / "skeino.db"
    graph = FakeGraph()
    graph.invoke_gate = asyncio.Event()
    async with _running(_app(db, graph, orphaned_run_timeout_seconds=None)) as ops:
        live = await ops.create_run(
            str(uuid4()),
            RunCreateRequest(
                assistant_id="agent", input={"messages": []}, if_not_exists="create"
            ),
        )
        await graph.invoke_started.wait()
        thread_id = str(live.thread_id)
        orphan = str(uuid4())
        store = ops._metadata_store
        await store.create_run(
            orphan,
            thread_id,
            "agent",
            metadata={},
            kwargs={},
            multitask_strategy="enqueue",
        )
        old = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        async with store._lock:
            await store._conn.execute(
                "UPDATE app_runs SET updated_at = ? WHERE run_id = ?", (old, orphan)
            )
            await store._conn.commit()

        assert await ops.fail_orphaned_runs(stale_after_seconds=TIMEOUT) == [orphan]
        assert (await store.fetch_thread_row(thread_id))["status"] == "busy"
        graph.invoke_gate.set()
        await ops.join_run(thread_id, str(live.run_id))


async def test_a_failing_sweep_is_logged_not_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with _running(_app(tmp_path / "skeino.db", FakeGraph())) as ops:

        async def boom(**_kwargs: Any) -> list[Any]:
            raise sqlite3.OperationalError("database is locked")

        ops._metadata_store.fail_stale_runs = boom
        await ops.liveness_pass(stale_after_seconds=TIMEOUT)  # does not raise
    assert "orphan sweep failed" in caplog.text


@pytest.mark.parametrize(
    ("heartbeat", "timeout"), [(30.0, 60.0), (30.0, 10.0)], ids=["2x", "below"]
)
def test_orphan_timeout_must_outlast_two_heartbeats(
    heartbeat: float, timeout: float
) -> None:
    with pytest.raises(ValueError, match="orphaned_run_timeout_seconds"):
        SkeinoSettings(
            run_heartbeat_seconds=heartbeat, orphaned_run_timeout_seconds=timeout
        )


def test_orphan_sweep_can_be_disabled() -> None:
    assert SkeinoSettings(orphaned_run_timeout_seconds=None)
