"""Runs orphaned by a dead process are failed, not left ``running`` forever.

A run's task lives in the process that started it. When that process dies
without a graceful shutdown, its rows stay ``pending``/``running``, its threads
stay ``busy``, and pollers wait forever. Each process heartbeats the runs it owns;
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

import httpx
import pytest
from fastapi import FastAPI
from langgraph.types import Interrupt
from mongomock_motor import AsyncMongoMockClient

from skeino import SkeinoSettings, create_app
from skeino.persistence import MongoMetadataStore
from skeino.schemas import RunCreateRequest
from tests.conftest import FakeGraph, build_test_app

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
    # A wide timeout: the live run must survive a slow CI runner delaying A's
    # heartbeat, which a 0.3s window (six heartbeats) did not always allow.
    timeout = 1.5
    async with (
        _running(_app(db, graph_a, orphaned_run_timeout_seconds=timeout)) as worker_a,
        _running(
            _app(db, FakeGraph(), orphaned_run_timeout_seconds=timeout)
        ) as worker_b,
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

        # The dead run goes stale while both workers sweep the shared store.
        await asyncio.sleep(timeout * 1.5)

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


@pytest.mark.parametrize("settled", ["idle", "interrupted"])
async def test_sweep_leaves_a_thread_a_later_run_already_settled(
    tmp_path: Path, settled: str
) -> None:
    # A quick restart skips a not-yet-stale orphan; a new run then finishes on
    # the thread. The later sweep fails the orphan but must not turn the
    # settled thread to "error" (an interrupted one would lose its resume).
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        thread_id, run_id = await _crashed_run(ops, age_seconds=3600)
        await ops._metadata_store.update_thread(thread_id, status_value=settled)
        assert await ops.fail_orphaned_runs(stale_after_seconds=TIMEOUT) == [run_id]
        assert (await _status(ops, thread_id, run_id))[0] == "error"
        thread = await ops._metadata_store.fetch_thread_row(thread_id)
        assert thread["status"] == settled


async def test_one_failing_thread_release_does_not_strand_the_others(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        broken_thread, broken_run = await _crashed_run(ops, age_seconds=3600)
        other_thread, other_run = await _crashed_run(ops, age_seconds=3600)
        store = ops._metadata_store
        fetch = store.fetch_thread_row

        async def flaky(thread_id: str) -> Any:
            if thread_id == broken_thread:
                raise sqlite3.OperationalError("database is locked")
            return await fetch(thread_id)

        store.fetch_thread_row = flaky
        # Runs are stale (an hour old); the threads just went busy. A window
        # wider than any CI stall keeps the stuck-thread backstop from
        # releasing the broken thread, so only the in-process retry can.
        stale = 60
        failed = await ops.fail_orphaned_runs(stale_after_seconds=stale)
        store.fetch_thread_row = fetch
        assert sorted(failed) == sorted([broken_run, other_run])
        assert (await fetch(other_thread))["status"] == "error"
        assert "Failed to release thread" in caplog.text
        assert (await fetch(broken_thread))["status"] == "busy"

        # The claimed run is never returned again, so the next pass must
        # retry the release itself rather than wait for a sweep that never comes.
        again = await ops.fail_orphaned_runs(stale_after_seconds=stale)
        assert again == []
        assert (await fetch(broken_thread))["status"] == "error"
        assert ops._unreleased_threads == set()


async def test_sqlite_claim_loses_to_a_heartbeat_after_the_scan(
    tmp_path: Path,
) -> None:
    # Another process sharing the file heartbeats the run between this
    # sweeper's scan and its claim: the claim must not fail the live run.
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        thread_id, run_id = await _crashed_run(ops, age_seconds=3600)
        store = ops._metadata_store
        execute = store._conn.execute

        async def heartbeat_before_claim(sql: str, *args: Any) -> Any:
            if sql.startswith("UPDATE app_runs SET status = 'error'"):
                await execute(
                    "UPDATE app_runs SET updated_at = ? WHERE run_id = ?",
                    (datetime.now(UTC).isoformat(), run_id),
                )
            return await execute(sql, *args)

        store._conn.execute = heartbeat_before_claim
        try:
            assert await ops.fail_orphaned_runs(stale_after_seconds=TIMEOUT) == []
        finally:
            store._conn.execute = execute
        assert (await _status(ops, thread_id, run_id))[0] == "running"


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
    ("heartbeat", "timeout"),
    [(30.0, 89.9), (30.0, 60.0), (30.0, 10.0)],
    ids=["just-under-3x", "2x", "below"],
)
def test_orphan_timeout_must_cover_three_heartbeats(
    heartbeat: float, timeout: float
) -> None:
    with pytest.raises(ValueError, match="orphaned_run_timeout_seconds"):
        SkeinoSettings(
            run_heartbeat_seconds=heartbeat, orphaned_run_timeout_seconds=timeout
        )


def test_orphan_timeout_of_exactly_three_heartbeats_is_accepted() -> None:
    settings = SkeinoSettings(
        run_heartbeat_seconds=30.0, orphaned_run_timeout_seconds=90.0
    )
    assert settings.orphaned_run_timeout_seconds == 90.0


def test_orphan_sweep_can_be_disabled() -> None:
    assert SkeinoSettings(orphaned_run_timeout_seconds=None)


# --- streaming runs (registry-tracked producer tasks, #134) -----------------


async def test_sweep_spares_another_workers_live_streaming_run(tmp_path: Path) -> None:
    # A streaming run executes in a registry-tracked producer task, so its
    # owner heartbeats it like a background run: B's sweeps must not fail it.
    db = tmp_path / "skeino.db"
    graph_a = FakeGraph()
    graph_a.stream_gate = asyncio.Event()
    # Wide timeout, as in the background-run sibling: a 0.3s window flaked on a
    # slow CI runner delaying A's heartbeat.
    timeout = 1.5
    async with (
        _running(_app(db, graph_a, orphaned_run_timeout_seconds=timeout)) as worker_a,
        _running(
            _app(db, FakeGraph(), orphaned_run_timeout_seconds=timeout)
        ) as worker_b,
    ):
        thread_id = str(uuid4())
        run, events = await worker_a.create_streaming_run(
            thread_id,
            RunCreateRequest(
                assistant_id="agent",
                input={"messages": []},
                if_not_exists="create",
                stream_mode=["updates", "values"],
                stream_resumable=True,
            ),
        )
        run_id = str(run.run_id)
        drained = asyncio.create_task(_drain(events))
        await asyncio.wait_for(graph_a.stream_started.wait(), 5)

        # Past the timeout, so without A's heartbeat B would have swept it.
        await asyncio.sleep(timeout * 1.5)
        assert (await _status(worker_b, thread_id, run_id))[0] == "running"

        graph_a.stream_gate.set()
        body = await asyncio.wait_for(drained, 5)
        assert "event: end" in body
        assert (await _status(worker_a, thread_id, run_id))[0] == "success"


async def test_joining_a_swept_run_reports_the_orphaned_error(tmp_path: Path) -> None:
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        thread_id, run_id = await _crashed_run(ops, age_seconds=3600)

    app = _app(db, FakeGraph())
    async with _running(app) as ops:  # startup pass fails the orphan
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            joined = await client.get(
                f"/threads/{thread_id}/runs/{run_id}/stream",
                headers={"Last-Event-ID": "-1"},
            )
            assert joined.status_code == 200
            assert joined.text.startswith("event: error\n")
            assert "orphaned" in joined.text

            waited = await client.get(f"/threads/{thread_id}/runs/{run_id}/join")
            assert waited.status_code == 500
            assert "orphaned" in waited.json()["detail"]


async def _drain(events: Any) -> str:
    return "".join([chunk async for chunk in events])


async def _stuck_thread(
    run_ops: Any, *, run_status: str, thread_age_seconds: float
) -> str:
    """A ``busy`` thread whose only run is already final: a process died after
    saving the run's outcome but before moving the thread off ``busy``."""
    thread_id, run_id = await _crashed_run(run_ops, age_seconds=3600)
    store = run_ops._metadata_store
    await store.update_run_status(run_id, run_status)
    old = (datetime.now(UTC) - timedelta(seconds=thread_age_seconds)).isoformat()
    async with store._lock:
        await store._conn.execute(
            "UPDATE app_threads SET updated_at = ? WHERE thread_id = ?",
            (old, thread_id),
        )
        await store._conn.commit()
    return thread_id


@pytest.mark.parametrize(
    ("run_status", "released_to"),
    [("error", "error"), ("success", "idle"), ("interrupted", "idle")],
)
async def test_restart_releases_a_thread_left_busy_after_its_run_finished(
    tmp_path: Path, run_status: str, released_to: str
) -> None:
    # The sweeper (or the run's own process) died between saving the run's
    # outcome and releasing its thread. No row is left to re-claim and the
    # in-process retry record died with it: only the store says what happened.
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        stuck = await _stuck_thread(ops, run_status=run_status, thread_age_seconds=60)
        fresh = await _stuck_thread(ops, run_status=run_status, thread_age_seconds=0)

    # A 30s window: the stuck thread (60s) is past it, the fresh one is not,
    # however slow the restart.
    async with _running(_app(db, FakeGraph(), orphaned_run_timeout_seconds=30)) as ops:
        store = ops._metadata_store
        released = await store.fetch_thread_row(stuck)
        assert released["status"] == released_to
        # As the run's own settle would have: only a success changed the state.
        assert (released["state_updated_at"] is not None) == (run_status == "success")
        # Not busy for longer than the timeout yet: it may still be settling.
        assert (await store.fetch_thread_row(fresh))["status"] == "busy"


async def test_restart_releases_a_thread_left_busy_after_a_run_paused_on_interrupt(
    tmp_path: Path,
) -> None:
    # A run that succeeded parked on ``interrupt()`` leaves its thread
    # ``interrupted``, not ``idle``: the release reads the pause from the
    # graph state, as the run's own settle would have, or the resume is lost.
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        stuck = await _stuck_thread(ops, run_status="success", thread_age_seconds=60)

    paused = FakeGraph()
    paused.pending_interrupts = (Interrupt(value={"question": "approve?"}),)
    async with _running(_app(db, paused)) as ops:
        released = await ops._metadata_store.fetch_thread_row(stuck)
        assert released["status"] == "interrupted"
        assert released["state_updated_at"] is not None


async def test_stuck_thread_release_spares_a_thread_with_a_run_in_flight(
    tmp_path: Path,
) -> None:
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        thread_id = await _stuck_thread(ops, run_status="error", thread_age_seconds=60)
        store = ops._metadata_store
        # A second run on the thread, still in flight: fresh, so inside the
        # timeout window, not stale.
        await store.create_run(
            str(uuid4()),
            thread_id,
            "agent",
            metadata={},
            kwargs={},
            multitask_strategy="enqueue",
        )
        # The stuck thread (busy 60s) is past the timeout; the new run is not.
        await ops.fail_orphaned_runs(stale_after_seconds=30)
        assert (await store.fetch_thread_row(thread_id))["status"] == "busy"


async def test_stuck_thread_release_reads_mongos_naive_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Mongo hands timestamps back naive (UTC): the release must still compare
    # them with its aware cutoff instead of raising and stranding every thread.
    import motor.motor_asyncio

    monkeypatch.setattr(motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient)
    store = MongoMetadataStore("mongodb://mock", db_name=f"c{uuid4().hex}")
    await store.setup()
    app, _graph = build_test_app(orphaned_run_timeout_seconds=None)
    try:
        async with app.router.lifespan_context(app):
            ops = app.state.skeino.run_ops
            ops._metadata_store = store
            stuck, fresh = str(uuid4()), str(uuid4())
            for thread_id, age_seconds in ((stuck, 60), (fresh, 0)):
                await store.create_thread(
                    thread_id, metadata={}, config={}, ttl=None, if_exists="raise"
                )
                run_id = str(uuid4())
                await store.create_run(
                    run_id,
                    thread_id,
                    "agent",
                    metadata={},
                    kwargs={},
                    multitask_strategy="enqueue",
                )
                await store.update_run_status(run_id, "success")
                await store.update_thread(thread_id, status_value="busy")
                await store._threads.update_one(
                    {"_id": thread_id},
                    {
                        "$set": {
                            "updated_at": datetime.now(UTC)
                            - timedelta(seconds=age_seconds)
                        }
                    },
                )
            # The premise: this backend really returns naive timestamps.
            assert (await store.fetch_thread_row(stuck))["updated_at"].tzinfo is None

            await ops.fail_orphaned_runs(stale_after_seconds=30)

            released = await store.fetch_thread_row(stuck)
            assert released["status"] == "idle"
            assert released["state_updated_at"] is not None
            assert (await store.fetch_thread_row(fresh))["status"] == "busy"
    finally:
        await store.aclose()


async def test_sqlite_sweep_queries_use_an_index(tmp_path: Path) -> None:
    # The sweep runs every heartbeat while holding the store lock: its scans
    # must stay the size of the in-flight set, not of the whole history.
    db = tmp_path / "skeino.db"
    async with _running(
        _app(db, FakeGraph(), orphaned_run_timeout_seconds=None)
    ) as ops:
        await _crashed_run(ops, age_seconds=3600)
        store = ops._metadata_store
        execute = store._conn.execute
        selects: list[tuple[str, Any]] = []

        def record(sql: str, parameters: Any = None) -> Any:
            if sql.lstrip().startswith("SELECT"):
                selects.append((sql, parameters))
            return execute(sql, parameters)

        store._conn.execute = record
        await ops.fail_orphaned_runs(stale_after_seconds=TIMEOUT)
        store._conn.execute = execute

        plans = {}
        for sql, parameters in selects:
            if "FROM app_runs WHERE status IN" in sql or "FROM app_threads" in sql:
                cursor = await execute(f"EXPLAIN QUERY PLAN {sql}", parameters or ())
                plans[sql] = " | ".join(str(row[-1]) for row in await cursor.fetchall())
        assert any("idx_app_runs_inflight_updated" in p for p in plans.values()), plans
        assert any("idx_app_threads_status_updated" in p for p in plans.values()), plans
        # Walking a partial index (``SCAN … USING INDEX``) reads only the
        # in-flight rows; a bare ``SCAN app_…`` reads the whole table.
        assert not any(
            step.startswith("SCAN app_") and "USING" not in step
            for plan in plans.values()
            for step in plan.split(" | ")
        ), plans
