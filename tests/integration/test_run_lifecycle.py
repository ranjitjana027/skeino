"""Run failure, cancellation, and stream-retry lifecycle behaviour.

These exercise the error/cancel paths that the cooperative ``FakeGraph`` could
not reach before failure injection was added: a failed run must release its
thread lock and record an ``error`` status; a streaming failure must emit an
``error`` event without replaying already-sent output; a client disconnect must
mark the run ``interrupted`` and free the lock when ``on_disconnect="cancel"``,
and leave it running to completion when ``"continue"``.
"""

import asyncio
from typing import Any

import pytest
from fastapi.testclient import TestClient

from skeino.schemas import RunCreateRequest
from tests.conftest import FakeGraph, build_test_app


def _new_thread(client: TestClient) -> str:
    return client.post("/threads", json={}).json()["thread_id"]


def _event_names(text: str) -> list[str]:
    chunks = [c for c in text.split("\n\n") if c.strip()]
    return [
        next(line for line in chunk.splitlines() if line.startswith("event: "))[7:]
        for chunk in chunks
    ]


def _reject_run_still_acquires(client: TestClient, thread_id: str) -> int:
    """Issue a reject-strategy run; 200 proves the thread lock was released."""
    return client.post(
        f"/threads/{thread_id}/runs",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": []},
            "multitask_strategy": "reject",
        },
    ).status_code


def test_failed_sync_run_releases_lock_and_records_error() -> None:
    app, graph = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        graph.invoke_error = RuntimeError("graph boom")
        # /runs/wait awaits the background task, so the graph failure surfaces
        # to the caller as a 500.
        r = client.post(
            f"/threads/{thread_id}/runs/wait",
            json={"assistant_id": "test_agent", "input": {"messages": []}},
        )
        assert r.status_code == 500

        errored = client.get(f"/threads/{thread_id}/runs?status=error").json()
        assert len(errored) == 1

        # A reject-strategy run now succeeds → the lock was released on failure.
        graph.invoke_error = None
        assert _reject_run_still_acquires(client, thread_id) == 200


def test_streaming_error_emits_error_event_and_releases_lock() -> None:
    app, graph = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        graph.stream_error = ValueError("deterministic boom")  # not retriable
        with client.stream(
            "POST",
            f"/threads/{thread_id}/runs/stream",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "stream_mode": "values",
            },
        ) as r:
            text = "".join(r.iter_text())

        events = _event_names(text)
        assert events[0] == "metadata"
        assert events[-1] == "error"
        assert graph.stream_attempts == 1  # non-retriable → no retry

        errored = client.get(f"/threads/{thread_id}/runs?status=error").json()
        assert len(errored) == 1

        graph.stream_error = None
        assert _reject_run_still_acquires(client, thread_id) == 200


def test_streaming_does_not_replay_after_first_event() -> None:
    app, graph = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        # Retriable by message, but raised *after* one event has been sent.
        graph.stream_error = TimeoutError("connection timed out")
        graph.stream_error_after = 1
        with client.stream(
            "POST",
            f"/threads/{thread_id}/runs/stream",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "stream_mode": "values",
            },
        ) as r:
            text = "".join(r.iter_text())

        events = _event_names(text)
        # The single emitted value is NOT replayed, and the run surfaces as error.
        assert events.count("values") == 1
        assert events[-1] == "error"
        assert graph.stream_attempts == 1  # no retry once output was sent


def test_streaming_retries_before_first_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("skeino.ops.runs.STREAM_RETRY_BACKOFF_SECS", 0.0)
    app, graph = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        # First attempt fails with a retriable error before emitting anything;
        # the second attempt succeeds.
        graph.stream_error = ConnectionError("connection reset by peer")
        graph.stream_fail_times = 1
        with client.stream(
            "POST",
            f"/threads/{thread_id}/runs/stream",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "stream_mode": "values",
            },
        ) as r:
            text = "".join(r.iter_text())

        events = _event_names(text)
        assert events[-1] == "end"
        assert graph.stream_attempts == 2  # retried once, then succeeded


async def _disconnect_mid_stream(on_disconnect: str) -> tuple[Any, Any, str, str]:
    """Start a gated streaming run, read one event, then drop the client."""
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    graph.stream_gate = asyncio.Event()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        thread_id = "11111111-1111-1111-1111-111111111111"
        request = RunCreateRequest(
            assistant_id="test_agent",
            input={"messages": []},
            if_not_exists="create",
            stream_mode=["updates", "values"],
            on_disconnect=on_disconnect,
        )
        run, stream = await run_ops.create_streaming_run(thread_id, request)
        task = run_ops._registry.get(str(run.run_id))
        assert task is not None

        first = await stream.__anext__()
        assert "event: metadata" in first
        await graph.stream_started.wait()  # the run is now parked mid-graph

        # Simulate a client disconnect mid-stream.
        with pytest.raises(asyncio.CancelledError):
            await stream.athrow(asyncio.CancelledError())

        graph.stream_gate.set()  # let a still-running run finish
        await asyncio.wait({task})
        persisted = await run_ops.get_run(thread_id, str(run.run_id))
        locked = run_ops._lock_manager.get(thread_id).locked()
        return persisted, graph, persisted.status, "locked" if locked else "free"


async def test_disconnect_with_on_disconnect_cancel_interrupts_run() -> None:
    _, _, run_status, lock = await _disconnect_mid_stream("cancel")
    assert run_status == "interrupted"
    assert lock == "free"


async def test_disconnect_with_on_disconnect_continue_finishes_run() -> None:
    # The LangGraph default: the run outlives its client, so it can be joined.
    _, graph, run_status, lock = await _disconnect_mid_stream("continue")
    assert run_status == "success"
    assert lock == "free"
    state = graph.state_by_thread["11111111-1111-1111-1111-111111111111"]
    assert state["messages"][-1]["content"] == "streamed"
