"""``GET /threads/{thread_id}/runs/{run_id}/stream`` — join a run's event stream.

The LangGraph SDK's ``client.runs.joinStream`` (and ``useStream``'s
``joinStream`` / ``reconnectOnMount``) re-attaches to a run after the page that
started it went away. These tests drive that through the real route:

* a resumable run replays after ``Last-Event-ID`` (``-1`` = from the start)
  with the ids the original stream carried, then tails live events;
* ``stream_mode`` filters what a join sees; lifecycle events always pass;
* a finished run with nothing retained yields its final state, then ``end``;
* unknown / foreign runs 404, malformed ``Last-Event-ID`` 422, an in-flight run
  with no stream in this process 409;
* ``cancel_on_disconnect`` decides whether a departing joiner cancels the run.

httpx's ``ASGITransport`` buffers a whole response, so a mid-run join is sent as
a task, the test waits until it has subscribed, and only then lets the run
continue — the events it gets after that point can only have come live.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from skeino.schemas import RunCreateRequest
from tests.conftest import FakeGraph, build_test_app

_THREAD = "22222222-2222-2222-2222-222222222222"
_OTHER_THREAD = "33333333-3333-3333-3333-333333333333"

Frame = tuple[int | None, str, Any]


def parse_frames(body: str) -> list[Frame]:
    """Split an SSE body into ``(id, event, data)`` triples, in order."""
    frames: list[Frame] = []
    for chunk in body.split("\n\n"):
        event_id: int | None = None
        name: str | None = None
        data: Any = None
        for line in chunk.splitlines():
            if line.startswith("id: "):
                event_id = int(line[4:])
            elif line.startswith("event: "):
                name = line[7:]
            elif line.startswith("data: "):
                data = json.loads(line[6:])
        if name is not None:
            frames.append((event_id, name, data))
    return frames


@asynccontextmanager
async def running_app(
    **settings: Any,
) -> AsyncIterator[tuple[FastAPI, FakeGraph, httpx.AsyncClient]]:
    app, graph = build_test_app(**settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            yield app, graph, client


def _request(**overrides: Any) -> RunCreateRequest:
    fields: dict[str, Any] = {
        "assistant_id": "test_agent",
        "input": {"messages": [{"type": "human", "content": "hi"}]},
        "if_not_exists": "create",
        "stream_mode": ["updates", "values"],
        "stream_resumable": True,
    }
    fields.update(overrides)
    return RunCreateRequest(**fields)


async def _start_and_leave(
    app: FastAPI, graph: FakeGraph, **overrides: Any
) -> tuple[str, asyncio.Task[Any]]:
    """Start a gated streaming run, read its first event, then 'leave the page'.

    The run is parked after its first graph event (``updates``), so events
    1 (metadata) and 2 (updates) are published and 3 (values) / 4 (end) are
    still to come.
    """
    graph.stream_gate = asyncio.Event()
    run_ops = app.state.skeino.run_ops
    run, original = await run_ops.create_streaming_run(_THREAD, _request(**overrides))
    run_id = str(run.run_id)
    task = run_ops._registry.get(run_id)
    assert task is not None
    assert "event: metadata" in await original.__anext__()
    await graph.stream_started.wait()
    await original.aclose()  # the client goes away; on_disconnect=continue
    return run_id, task


async def _join_while_parked(
    app: FastAPI,
    graph: FakeGraph,
    client: httpx.AsyncClient,
    run_id: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> httpx.Response:
    """Join a parked run, then release it once the join has subscribed."""
    stream = app.state.skeino.run_ops._streams.get(_THREAD, run_id)
    assert stream is not None
    before = stream.subscriber_count
    join = asyncio.create_task(
        client.get(
            f"/threads/{_THREAD}/runs/{run_id}/stream",
            headers=headers or {},
            params=params or {},
        )
    )
    for _ in range(200):
        if stream.subscriber_count > before:
            break
        await asyncio.sleep(0.005)
    else:  # pragma: no cover - diagnostic only
        join.cancel()
        pytest.fail("join never subscribed to the run's event stream")
    assert not join.done()
    assert graph.stream_gate is not None
    graph.stream_gate.set()
    return await asyncio.wait_for(join, 5)


# --- replay + live tail ----------------------------------------------------


async def test_join_mid_run_replays_from_start_then_tails_live() -> None:
    async with running_app() as (app, graph, client):
        run_id, task = await _start_and_leave(app, graph)
        r = await _join_while_parked(
            app, graph, client, run_id, headers={"Last-Event-ID": "-1"}
        )
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        assert r.headers["content-location"] == f"/threads/{_THREAD}/runs/{run_id}"
        frames = parse_frames(r.text)
        # Replayed (1, 2) and live (3, 4), with the original stream's ids.
        assert [(i, e) for i, e, _ in frames] == [
            (1, "metadata"),
            (2, "updates"),
            (3, "values"),
            (4, "end"),
        ]
        assert frames[0][2]["run_id"] == run_id
        assert frames[2][2]["messages"][-1]["content"] == "streamed"
        assert frames[3][2]["status"] == "success"
        await asyncio.wait({task})


async def test_join_resumes_after_last_event_id_without_duplicates() -> None:
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph)
        r = await _join_while_parked(
            app, graph, client, run_id, headers={"Last-Event-ID": "2"}
        )
        frames = parse_frames(r.text)
        assert [(i, e) for i, e, _ in frames] == [(3, "values"), (4, "end")]


async def test_join_with_cursor_older_than_retained_window_is_409() -> None:
    async with running_app(resumable_stream_max_events=2) as (app, _graph, client):
        run, original = await app.state.skeino.run_ops.create_streaming_run(
            _THREAD, _request()
        )
        _ = [frame async for frame in original]

        response = await client.get(
            f"/threads/{_THREAD}/runs/{run.run_id}/stream",
            headers={"Last-Event-ID": "-1"},
        )

        assert response.status_code == 409
        assert "predates the retained event window" in response.json()["detail"]


async def test_join_without_last_event_id_tails_live_only() -> None:
    # LangGraph semantics: no Last-Event-ID → no replay, only what comes next.
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph)
        r = await _join_while_parked(app, graph, client, run_id)
        frames = parse_frames(r.text)
        assert [(i, e) for i, e, _ in frames] == [(3, "values"), (4, "end")]


async def test_join_of_non_resumable_run_has_no_history_to_replay() -> None:
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph, stream_resumable=False)
        r = await _join_while_parked(
            app, graph, client, run_id, headers={"Last-Event-ID": "-1"}
        )
        frames = parse_frames(r.text)
        # Events 1-2 were published before the join and kept nowhere.
        assert [(i, e) for i, e, _ in frames] == [(3, "values"), (4, "end")]


@pytest.mark.parametrize(
    "stream_mode",
    ["values", '["values"]', '["values","messages-tuple"]'],
)
async def test_join_filters_by_stream_mode_but_keeps_lifecycle_events(
    stream_mode: str,
) -> None:
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph)
        r = await _join_while_parked(
            app,
            graph,
            client,
            run_id,
            headers={"Last-Event-ID": "-1"},
            params={"stream_mode": stream_mode},
        )
        frames = parse_frames(r.text)
        assert [(i, e) for i, e, _ in frames] == [
            (1, "metadata"),
            (3, "values"),
            (4, "end"),
        ]


async def test_join_accepts_repeated_stream_mode_params() -> None:
    async with running_app() as (app, graph, client):
        run_id, task = await _start_and_leave(app, graph)
        graph.stream_gate.set()  # type: ignore[union-attr]
        await asyncio.wait({task})
        r = await client.get(
            f"/threads/{_THREAD}/runs/{run_id}/stream?stream_mode=updates&stream_mode=values",
            headers={"Last-Event-ID": "-1"},
        )
        events = [e for _, e, _ in parse_frames(r.text)]
        assert events == ["metadata", "updates", "values", "end"]


# --- finished runs -----------------------------------------------------------


async def test_join_finished_resumable_run_replays_retained_events() -> None:
    async with running_app() as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(_THREAD, _request())
        original_frames = parse_frames("".join([f async for f in original]))
        r = await client.get(
            f"/threads/{_THREAD}/runs/{run.run_id}/stream",
            headers={"Last-Event-ID": "-1"},
        )
        assert parse_frames(r.text) == original_frames
        assert [i for i, _, _ in original_frames] == [1, 2, 3, 4]


async def test_join_finished_run_without_last_event_id_sends_final_state() -> None:
    async with running_app() as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(_THREAD, _request())
        _ = [f async for f in original]
        r = await client.get(f"/threads/{_THREAD}/runs/{run.run_id}/stream")
        frames = parse_frames(r.text)
        assert [(i, e) for i, e, _ in frames] == [(None, "values"), (None, "end")]
        assert frames[0][2]["messages"][-1]["content"] == "streamed"
        assert frames[1][2] == {"run_id": str(run.run_id), "status": "success"}


@pytest.mark.parametrize(
    ("settings", "resumable"),
    [
        ({}, False),
        ({"resumable_stream_ttl_seconds": 0}, True),
        ({"resumable_stream_max_retained_runs": 0}, True),
    ],
    ids=["non-resumable", "retention-expired", "retention-disabled"],
)
async def test_join_finished_run_with_nothing_retained_sends_final_state(
    settings: dict[str, Any], resumable: bool
) -> None:
    async with running_app(**settings) as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(
            _THREAD, _request(stream_resumable=resumable)
        )
        _ = [f async for f in original]
        r = await client.get(
            f"/threads/{_THREAD}/runs/{run.run_id}/stream",
            headers={"Last-Event-ID": "-1"},
        )
        frames = parse_frames(r.text)
        assert [(i, e) for i, e, _ in frames] == [(None, "values"), (None, "end")]


async def test_join_finished_run_filtered_away_from_values_sends_only_end() -> None:
    async with running_app() as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(
            _THREAD, _request(stream_resumable=False)
        )
        _ = [f async for f in original]
        r = await client.get(
            f"/threads/{_THREAD}/runs/{run.run_id}/stream",
            params={"stream_mode": "updates"},
        )
        assert [e for _, e, _ in parse_frames(r.text)] == ["end"]


async def test_join_failed_run_reports_error() -> None:
    async with running_app() as (app, graph, client):
        graph.stream_error = ValueError("graph exploded")
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(
            _THREAD, _request(stream_resumable=False)
        )
        assert [e for _, e, _ in parse_frames("".join([f async for f in original]))][
            -1
        ] == "error"
        r = await client.get(f"/threads/{_THREAD}/runs/{run.run_id}/stream")
        frames = parse_frames(r.text)
        assert [e for _, e, _ in frames] == ["error"]
        assert "graph exploded" in frames[0][2]["detail"]


async def test_join_background_run_waits_then_sends_final_state() -> None:
    # POST /runs executes without streaming, so there is no event history;
    # joining it waits for the run and then reports its final state.
    async with running_app() as (app, graph, client):
        graph.invoke_gate = asyncio.Event()
        created = await client.post(
            f"/threads/{_THREAD}/runs",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "if_not_exists": "create",
            },
        )
        run_id = created.json()["run_id"]
        await graph.invoke_started.wait()
        join = asyncio.create_task(
            client.get(f"/threads/{_THREAD}/runs/{run_id}/stream")
        )
        await asyncio.sleep(0.05)
        assert not join.done()  # still waiting on the run
        graph.invoke_gate.set()
        frames = parse_frames((await join).text)
        assert [e for _, e, _ in frames] == ["values", "end"]
        assert frames[0][2]["messages"][-1]["content"] == "completed"


# --- status codes ------------------------------------------------------------


async def test_join_unknown_run_is_404() -> None:
    async with running_app() as (_, _graph, client):
        await client.post("/threads", json={"thread_id": _THREAD})
        r = await client.get(
            f"/threads/{_THREAD}/runs/44444444-4444-4444-4444-444444444444/stream"
        )
        assert r.status_code == 404


async def test_join_unknown_thread_is_404() -> None:
    async with running_app() as (_, _graph, client):
        r = await client.get(
            f"/threads/{_THREAD}/runs/44444444-4444-4444-4444-444444444444/stream"
        )
        assert r.status_code == 404


async def test_join_run_under_another_thread_is_404_and_leaks_nothing() -> None:
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph)
        await client.post("/threads", json={"thread_id": _OTHER_THREAD})
        r = await client.get(
            f"/threads/{_OTHER_THREAD}/runs/{run_id}/stream",
            headers={"Last-Event-ID": "-1"},
        )
        assert r.status_code == 404
        assert "event:" not in r.text
        graph.stream_gate.set()  # type: ignore[union-attr]


async def test_join_with_malformed_last_event_id_is_422() -> None:
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph)
        r = await client.get(
            f"/threads/{_THREAD}/runs/{run_id}/stream",
            headers={"Last-Event-ID": "1700000000000-0"},
        )
        assert r.status_code == 422
        graph.stream_gate.set()  # type: ignore[union-attr]


async def test_join_with_malformed_stream_mode_is_422() -> None:
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph)
        for bad in ('["values"', "[1, 2]", "bogus", '["values", "bogus"]'):
            r = await client.get(
                f"/threads/{_THREAD}/runs/{run_id}/stream",
                params={"stream_mode": bad},
            )
            assert r.status_code == 422, bad
        graph.stream_gate.set()  # type: ignore[union-attr]


async def test_join_in_flight_run_with_no_stream_in_process_is_409() -> None:
    # A row left ``running`` with no task or stream here (another worker, or
    # stranded by a crash) has nothing to join.
    async with running_app() as (app, _graph, client):
        run_ops = app.state.skeino.run_ops
        run = await run_ops.create_run(_THREAD, _request())
        await run_ops.join_run(_THREAD, str(run.run_id))
        stranded = await run_ops._metadata_store.create_run(
            str(uuid4()),
            _THREAD,
            "test_agent",
            metadata={},
            kwargs={},
            multitask_strategy="enqueue",
        )
        await run_ops._metadata_store.update_run_status(
            str(stranded["run_id"]), "running"
        )
        r = await client.get(f"/threads/{_THREAD}/runs/{stranded['run_id']}/stream")
        assert r.status_code == 409


# --- disconnect behaviour ----------------------------------------------------


async def _join_then_disconnect(cancel_on_disconnect: bool) -> str:
    async with running_app() as (app, graph, _client):
        run_ops = app.state.skeino.run_ops
        run_id, task = await _start_and_leave(app, graph)
        joined = await run_ops.join_run_stream(
            _THREAD,
            run_id,
            stream_modes=[],
            last_event_id="-1",
            cancel_on_disconnect=cancel_on_disconnect,
        )
        # Bounded: a broken replay must fail the test, not hang the suite.
        first = await asyncio.wait_for(joined.__anext__(), 5)  # type: ignore[attr-defined]
        assert "event: metadata" in first
        with pytest.raises(asyncio.CancelledError):
            await joined.athrow(asyncio.CancelledError())  # type: ignore[attr-defined]
        assert graph.stream_gate is not None
        graph.stream_gate.set()
        await asyncio.wait({task})
        return (await run_ops.get_run(_THREAD, run_id)).status


async def test_joiner_disconnect_cancels_run_when_asked() -> None:
    assert await _join_then_disconnect(cancel_on_disconnect=True) == "interrupted"


async def test_joiner_disconnect_leaves_run_running_by_default() -> None:
    assert await _join_then_disconnect(cancel_on_disconnect=False) == "success"


async def test_cancel_on_disconnect_query_param_is_parsed() -> None:
    async with running_app() as (app, graph, client):
        run_id, _ = await _start_and_leave(app, graph)
        r = await client.get(
            f"/threads/{_THREAD}/runs/{run_id}/stream",
            params={"cancel_on_disconnect": "maybe"},
        )
        assert r.status_code == 422
        graph.stream_gate.set()  # type: ignore[union-attr]


async def test_streaming_run_can_be_cancelled_and_joiners_see_end() -> None:
    # Streaming runs are now tasks, so POST .../cancel works on them, and a
    # joined client learns the run ended rather than hanging.
    async with running_app() as (app, graph, client):
        run_id, task = await _start_and_leave(app, graph)
        stream = app.state.skeino.run_ops._streams.get(_THREAD, run_id)
        join = asyncio.create_task(
            client.get(
                f"/threads/{_THREAD}/runs/{run_id}/stream",
                headers={"Last-Event-ID": "2"},
            )
        )
        async with asyncio.timeout(5):
            while stream.subscriber_count == 0:
                await asyncio.sleep(0.005)
        r = await client.post(
            f"/threads/{_THREAD}/runs/{run_id}/cancel", params={"wait": "true"}
        )
        assert r.status_code == 204
        frames = parse_frames((await asyncio.wait_for(join, 5)).text)
        assert [(i, e, d["status"]) for i, e, d in frames] == [
            (3, "end", "interrupted")
        ]
        assert task.cancelled()


async def test_burst_stream_does_not_detach_a_subscriber_that_can_drain() -> None:
    async with running_app() as (app, _graph, _client):
        run_ops = app.state.skeino.run_ops

        async def burst(*_args: Any, **_kwargs: Any) -> AsyncIterator[tuple[str, Any]]:
            for index in range(300):
                yield "custom", {"index": index}

        run_ops._streamer.stream = burst
        run, events = await run_ops.create_streaming_run(_THREAD, _request())
        frames = parse_frames(await _drain(events))

        assert len([name for _, name, _ in frames if name == "custom"]) == 300


@pytest.mark.parametrize("stateless", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
async def test_overflow_reports_error_and_honors_disconnect_policy(
    stateless: bool,
    cancel: bool,
) -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        graph.stream_gate = asyncio.Event()
        request = _request(
            stream_resumable=False, on_disconnect="cancel" if cancel else "continue"
        )
        if stateless:
            run, events = await ops.create_stateless_streaming_run(request)
        else:
            run, events = await ops.create_streaming_run(_THREAD, request)
        task = ops._registry.get(str(run.run_id))
        assert task is not None
        await events.__anext__()
        await graph.stream_started.wait()
        stream = ops._streams._streams[str(run.run_id)]
        for index in range(257):
            stream.publish("custom", {"index": index})
        frames = parse_frames(await _drain(events))
        assert len(frames) == 1
        assert frames[0][0] is None
        assert frames[0][1] == "error"
        assert frames[0][2]["code"] == "subscriber_overflow"
        assert frames[0][2]["run_id"] == str(run.run_id)
        assert not stream.closed
        if cancel:
            await asyncio.wait({task})
            assert task.cancelled()
        else:
            assert not task.done()
            graph.stream_gate.set()
            await task


async def test_join_refreshes_status_when_local_handles_have_disappeared() -> None:
    async with running_app() as (app, _graph, _client):
        run_ops = app.state.skeino.run_ops
        run, events = await run_ops.create_streaming_run(
            _THREAD, _request(stream_resumable=False)
        )
        await _drain(events)
        await asyncio.sleep(0)
        final = await run_ops.get_run(_THREAD, str(run.run_id))
        run_ops.get_run = AsyncMock(
            side_effect=[final.model_copy(update={"status": "running"}), final]
        )
        joined = await run_ops.join_run_stream(
            _THREAD,
            str(run.run_id),
            stream_modes=[],
            last_event_id="2",
            cancel_on_disconnect=False,
        )
        frames = parse_frames(await _drain(joined))
        assert frames[-1][1] == "end"
        assert frames[-1][2]["status"] == "success"
        assert run_ops.get_run.await_count == 2


async def test_cancel_before_stream_task_starts_runs_stateless_cleanup() -> None:
    async with running_app() as (app, graph, _client):
        run_ops = app.state.skeino.run_ops
        run, _events = await run_ops.create_stateless_streaming_run(_request())
        stream = run_ops._streams._streams[str(run.run_id)]
        thread_id = stream.thread_id
        task = run_ops._registry.get(str(run.run_id))
        assert task is not None
        task.cancel()
        await asyncio.wait({task})
        # The done callback must persist interruption and run after_run even
        # though _publish_run never got its first event-loop turn.
        for _ in range(20):
            if thread_id not in graph.state_by_thread:
                break
            await asyncio.sleep(0)

        assert thread_id not in graph.state_by_thread
        assert thread_id not in graph.history_by_thread
        assert thread_id not in graph.checkpoints_by_thread
        assert not stream.resumable
        assert str(run.run_id) not in run_ops._streams._streams


async def test_overflow_cancels_once_while_interruption_write_is_pending() -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        graph.stream_gate = asyncio.Event()
        entered, release = asyncio.Event(), asyncio.Event()
        persist = ops._mark_run_interrupted

        async def delayed_interrupt(run_id: str, thread_id: str) -> None:
            entered.set()
            await release.wait()
            await persist(run_id, thread_id)

        ops._mark_run_interrupted = delayed_interrupt
        # Non-resumable: a resumable subscriber recovers from history instead.
        run, events = await ops.create_streaming_run(
            _THREAD, _request(on_disconnect="cancel", stream_resumable=False)
        )
        await events.__anext__()
        await graph.stream_started.wait()
        task = ops._registry.get(str(run.run_id))
        assert task is not None
        stream = ops._streams.get(_THREAD, str(run.run_id))
        assert stream is not None
        for index in range(257):
            stream.publish("custom", {"index": index})
        observer = stream.subscribe(after=None)
        assert "subscriber_overflow" in await events.__anext__()
        await entered.wait()
        joining = asyncio.create_task(ops.join_run(_THREAD, str(run.run_id)))
        await asyncio.sleep(0)
        assert not joining.done()
        await events.aclose()
        assert task.cancelling() == 1
        release.set()
        await ops._registry.wait(str(run.run_id))
        await joining
        terminal = [event async for event in observer]
        assert [event.event for event in terminal] == ["end"]
        assert (await ops.get_run(_THREAD, str(run.run_id))).status == "interrupted"


async def test_streaming_run_is_superseded_by_interrupt_strategy() -> None:
    async with running_app() as (app, graph, client):
        run_id, task = await _start_and_leave(app, graph)
        graph.stream_gate = None  # the next run streams straight through
        r = await client.post(
            f"/threads/{_THREAD}/runs/wait",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "multitask_strategy": "interrupt",
            },
        )
        assert r.status_code == 200
        assert task.cancelled()
        first = await client.get(f"/threads/{_THREAD}/runs/{run_id}")
        assert first.json()["status"] == "interrupted"


async def test_interrupt_cancels_stream_while_waiting_for_execution_lock() -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        lock = ops._lock_manager.get(_THREAD)
        await lock.acquire()
        queued = asyncio.create_task(
            ops.create_streaming_run(_THREAD, _request(multitask_strategy="enqueue"))
        )
        for _ in range(50):
            if len(ops._registry.all_active()) == 1:
                break
            await asyncio.sleep(0)
        else:
            pytest.fail("queued stream was not registered")

        graph.stream_gate = None
        replacement = asyncio.create_task(
            ops.create_streaming_run(_THREAD, _request(multitask_strategy="interrupt"))
        )
        await asyncio.sleep(0)
        lock.release()
        run, events = await asyncio.wait_for(replacement, 5)
        await _drain(events)
        with pytest.raises(HTTPException) as superseded:
            await queued
        assert superseded.value.status_code == 409
        assert "superseded" in superseded.value.detail
        assert (await ops.get_run(_THREAD, str(run.run_id))).status == "success"
        assert not lock.locked()


async def test_interrupt_supersedes_a_stream_while_its_row_is_being_inserted() -> None:
    # The lock is free but the row insert is slow: the run still has no
    # producer task, yet ``interrupt`` must cancel it rather than queue behind.
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        create_run = store.create_run
        inserting, never = asyncio.Event(), asyncio.Event()

        async def slow_first_insert(*args: Any, **kwargs: Any) -> Any:
            if not inserting.is_set():
                inserting.set()
                await never.wait()
            return await create_run(*args, **kwargs)

        store.create_run = slow_first_insert
        queued = asyncio.create_task(
            ops.create_streaming_run(_THREAD, _request(multitask_strategy="enqueue"))
        )
        await inserting.wait()
        graph.stream_gate = None
        run, events = await asyncio.wait_for(
            ops.create_streaming_run(_THREAD, _request(multitask_strategy="interrupt")),
            5,
        )
        await _drain(events)
        with pytest.raises(HTTPException) as superseded:
            await queued
        assert superseded.value.status_code == 409
        assert (await ops.get_run(_THREAD, str(run.run_id))).status == "success"
        assert not ops._lock_manager.get(_THREAD).locked()


async def test_join_reports_error_when_final_state_cannot_be_read() -> None:
    # A checkpointer outage must not turn into a clean, empty "success" stream.
    async with running_app() as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(
            _THREAD, _request(stream_resumable=False)
        )
        _ = [f async for f in original]

        async def outage(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("checkpointer down")

        graph.aget_state_history = None  # type: ignore[assignment]
        graph.aget_state = outage  # type: ignore[method-assign]
        r = await client.get(f"/threads/{_THREAD}/runs/{run.run_id}/stream")
        frames = parse_frames(r.text)
        assert [(i, e) for i, e, _ in frames] == [(None, "error")]
        assert frames[0][2]["run_id"] == str(run.run_id)
        assert "final state" in frames[0][2]["detail"]


def test_ops_join_rejects_bad_last_event_id_before_streaming() -> None:
    from skeino.ops.runs import _parse_last_event_id

    assert _parse_last_event_id(None) is None
    assert _parse_last_event_id("  ") is None
    assert _parse_last_event_id("-1") == -1
    assert _parse_last_event_id(" 7 ") == 7
    with pytest.raises(HTTPException) as exc:
        _parse_last_event_id("abc")
    assert exc.value.status_code == 422


# --- real compiled graph -------------------------------------------------------


async def test_join_real_graph_mid_run_matches_original_stream_exactly() -> None:
    """Against a real LangGraph graph: the joined stream (replay + live tail)
    is frame-for-frame what the original stream carried — same ids, same
    payloads — so a client resuming with ``Last-Event-ID`` cannot tell them
    apart."""
    from skeino import SkeinoSettings, create_app
    from tests.real_graphs import ASSISTANT_ID, make_gated, user_input

    entered, gate = asyncio.Event(), asyncio.Event()
    app = create_app(
        graphs={ASSISTANT_ID: make_gated(entered, gate)},
        settings=SkeinoSettings(default_assistant_id=ASSISTANT_ID),
    )
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(
            _THREAD,
            RunCreateRequest(
                assistant_id=ASSISTANT_ID,
                input=user_input("go"),
                if_not_exists="create",
                stream_mode=["values", "updates", "custom"],
                stream_resumable=True,
            ),
        )
        run_id = str(run.run_id)
        original_text = asyncio.create_task(_drain(original))
        await asyncio.wait_for(entered.wait(), 5)

        stream = run_ops._streams.get(_THREAD, run_id)
        published_before_join = stream._next_id - 1
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            join = asyncio.create_task(
                client.get(
                    f"/threads/{_THREAD}/runs/{run_id}/stream",
                    headers={"Last-Event-ID": "-1"},
                )
            )
            async with asyncio.timeout(5):
                while stream.subscriber_count < 2:
                    await asyncio.sleep(0.005)
            gate.set()
            joined = parse_frames((await asyncio.wait_for(join, 5)).text)

        expected = parse_frames(await asyncio.wait_for(original_text, 5))
        assert joined == expected
        ids = [i for i, _, _ in joined]
        assert ids == list(range(1, len(ids) + 1))
        # Some of it was replayed and some arrived live after the join.
        assert 1 < published_before_join < len(ids)
        names = [e for _, e, _ in joined]
        assert names[0] == "metadata" and names[-1] == "end"
        assert {"values", "updates", "custom"} <= set(names)
        final_values = [d for _, e, d in joined if e == "values"][-1]
        assert final_values["messages"][-1]["content"] == "step two after: step one"


async def _drain(events: AsyncIterator[str]) -> str:
    return "".join([chunk async for chunk in events])


async def test_stateless_response_waits_for_delayed_cleanup() -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        entered, release = asyncio.Event(), asyncio.Event()
        discard = ops._discard_thread

        async def delayed_discard(thread_id: str) -> None:
            entered.set()
            await release.wait()
            await discard(thread_id)

        ops._discard_thread = delayed_discard
        run, events = await ops.create_stateless_streaming_run(_request())
        response = asyncio.create_task(_drain(events))
        await entered.wait()
        await asyncio.sleep(0)
        finished_early = response.done()
        release.set()
        await response
        assert not finished_early
        assert not graph.state_by_thread
        assert str(run.run_id) not in ops._streams._streams


@pytest.mark.parametrize("resumable", [False, True])
async def test_no_cursor_join_during_finalization_returns_final_state(
    resumable: bool,
) -> None:
    # The producer is done (its ``end`` is published) but its finalizer has
    # not closed the stream yet: a live tail would see nothing.
    async with running_app() as (app, _graph, _client):
        ops = app.state.skeino.run_ops
        entered, release = asyncio.Event(), asyncio.Event()

        async def cleanup() -> None:
            entered.set()
            await release.wait()

        run, original = await ops.create_streaming_run(
            _THREAD, _request(stream_resumable=resumable), after_run=cleanup
        )
        response = asyncio.create_task(_drain(original))
        await entered.wait()
        task = ops._registry.get(str(run.run_id))
        assert task is not None and task.done()
        joined = await ops.join_run_stream(
            _THREAD,
            str(run.run_id),
            stream_modes=["values"],
            last_event_id=None,
            cancel_on_disconnect=False,
        )
        join_response = asyncio.create_task(_drain(joined))
        await asyncio.sleep(0)
        release.set()
        frames = parse_frames(await join_response)
        await response
        assert [name for _, name, _ in frames] == ["values", "end"]
        assert all(event_id is None for event_id, _, _ in frames)


# --- review follow-ups: final-state joins, background disconnects, shutdown ---


def _join_finished_real_run(graph_name: str) -> list[tuple[str, Any]]:
    """Run a real graph to completion over /runs/stream, then join it with no
    ``Last-Event-ID`` so the join synthesizes final-state events."""
    from tests.real_graphs import ASSISTANT_ID, parse_sse, real_client, user_input

    with real_client(graph_name) as client:
        thread_id = str(uuid4())
        client.post("/threads", json={"thread_id": thread_id})
        body = client.post(
            f"/threads/{thread_id}/runs/stream",
            json={
                "assistant_id": ASSISTANT_ID,
                "input": user_input(),
                "stream_mode": ["values"],
            },
        ).text
        run_id = next(d for n, d in parse_sse(body) if n == "metadata")["run_id"]
        joined = client.get(f"/threads/{thread_id}/runs/{run_id}/stream")
        assert joined.status_code == 200
        return parse_sse(joined.text)


@pytest.mark.parametrize("graph_name", ["typed_output", "pydantic_output"])
def test_join_finished_real_run_filters_final_state_by_output_schema(
    graph_name: str,
) -> None:
    events = _join_finished_real_run(graph_name)
    assert [n for n, _ in events] == ["values", "end"]
    values = events[0][1]
    assert "messages" in values
    assert "internal" not in values


def test_join_finished_real_run_paused_on_interrupt_carries_the_interrupt() -> None:
    events = _join_finished_real_run("interrupting")
    values = next(d for n, d in events if n == "values")
    assert values["__interrupt__"][0]["value"] == "approve?"


@pytest.mark.parametrize(
    ("cancel_on_disconnect", "expected"),
    [(True, "interrupted"), (False, "success")],
)
async def test_joiner_of_background_run_disconnect_honors_cancel_on_disconnect(
    cancel_on_disconnect: bool, expected: str
) -> None:
    async with running_app() as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        created = await client.post(
            f"/threads/{_THREAD}/runs",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "if_not_exists": "create",
            },
        )
        run_id = created.json()["run_id"]
        await graph.invoke_started.wait()
        task = run_ops._registry.get(run_id)
        assert task is not None
        joined = await run_ops.join_run_stream(
            _THREAD,
            run_id,
            stream_modes=[],
            last_event_id=None,
            cancel_on_disconnect=cancel_on_disconnect,
        )
        pending = asyncio.ensure_future(joined.__anext__())  # type: ignore[attr-defined]
        await asyncio.sleep(0.05)
        assert not pending.done()  # waiting on the background run
        pending.cancel()  # the joining client goes away
        with pytest.raises(asyncio.CancelledError):
            await pending
        graph.invoke_gate.set()
        await asyncio.wait({task}, timeout=5)
        assert task.done()
        assert (await run_ops.get_run(_THREAD, run_id)).status == expected


async def test_shutdown_interrupts_streaming_run_and_ends_its_subscribers() -> None:
    app, graph = build_test_app()
    graph.stream_gate = asyncio.Event()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        run, original = await run_ops.create_streaming_run(_THREAD, _request())
        run_id = str(run.run_id)
        assert "event: metadata" in await original.__anext__()
        await graph.stream_started.wait()
        stream = run_ops._streams.get(_THREAD, run_id)
        assert stream is not None
        joiner = stream.subscribe(after=-1)
        store = run_ops._metadata_store
    # Lifespan exit cancelled the parked producer and awaited its finalizer.
    events = await asyncio.wait_for(_events_of(joiner), 5)
    assert events[0].event == "metadata"
    assert events[-1].event == "end"
    assert parse_frames(events[-1].frame)[0][2]["status"] == "interrupted"
    row = await store.fetch_run_row(_THREAD, run_id)
    assert row is not None and row["status"] == "interrupted"
    assert not run_ops._lock_manager.get(_THREAD).locked()
    await original.aclose()


async def _events_of(subscription: AsyncIterator[Any]) -> list[Any]:
    return [event async for event in subscription]


# --- a producer that loses the terminal write to another worker's sweep -------


async def test_streaming_run_swept_by_another_worker_keeps_the_swept_outcome() -> None:
    async with running_app() as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        store = run_ops._metadata_store
        graph.stream_gate = asyncio.Event()
        run, original = await run_ops.create_streaming_run(_THREAD, _request())
        run_id = str(run.run_id)
        body = asyncio.create_task(_drain(original))
        await graph.stream_started.wait()
        # Another worker decides this run is orphaned and settles the thread.
        await store.update_run_status(run_id, "error", error="orphaned elsewhere")
        await store.update_thread(_THREAD, status_value="error")
        graph.stream_gate.set()
        frames = parse_frames(await asyncio.wait_for(body, 5))
        assert frames[-1][1] == "error"
        assert frames[-1][2] == {"detail": "orphaned elsewhere", "run_id": run_id}
        assert "end" not in [name for _, name, _ in frames]
        assert (await run_ops.get_run(_THREAD, run_id)).status == "error"
        thread = (await client.get(f"/threads/{_THREAD}")).json()
        assert thread["status"] == "error"


async def test_background_run_swept_by_another_worker_leaves_thread_alone() -> None:
    async with running_app() as (app, graph, client):
        run_ops = app.state.skeino.run_ops
        store = run_ops._metadata_store
        graph.invoke_gate = asyncio.Event()
        run = await run_ops.create_run(_THREAD, _request(stream_resumable=False))
        run_id = str(run.run_id)
        await graph.invoke_started.wait()
        task = run_ops._registry.get(run_id)
        assert task is not None
        await store.update_run_status(run_id, "error", error="orphaned elsewhere")
        await store.update_thread(_THREAD, status_value="error")
        graph.invoke_gate.set()
        await asyncio.wait_for(run_ops._registry.wait(run_id), 5)
        assert (await run_ops.get_run(_THREAD, run_id)).status == "error"
        thread = (await client.get(f"/threads/{_THREAD}")).json()
        assert thread["status"] == "error"


# --- cursors ahead of the run, fail-closed final state ------------------------


async def test_join_with_cursor_ahead_of_the_run_is_409() -> None:
    async with running_app() as (app, graph, client):
        run_id, _task = await _start_and_leave(app, graph)
        # Bounded: accepting the cursor would tail the parked run forever.
        r = await asyncio.wait_for(
            client.get(
                f"/threads/{_THREAD}/runs/{run_id}/stream",
                headers={"Last-Event-ID": "999"},
            ),
            5,
        )
        assert r.status_code == 409
        assert "ahead" in r.json()["detail"]
        assert graph.stream_gate is not None
        graph.stream_gate.set()


async def test_join_final_state_fails_closed_when_run_scoped_history_fails() -> None:
    # Falling back to the latest thread state could report a later run's state
    # as this run's output: report an error instead.
    async with running_app() as (app, graph, client):
        created = await client.post(
            f"/threads/{_THREAD}/runs/stream",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "if_not_exists": "create",
                "stream_mode": ["values"],
            },
        )
        run_id = parse_frames(created.text)[0][2]["run_id"]

        async def broken_history(*_args: Any, **_kwargs: Any) -> AsyncIterator[Any]:
            raise RuntimeError("history store down")
            yield  # pragma: no cover - makes this an async generator

        graph.aget_state_history = broken_history  # type: ignore[method-assign]
        joined = parse_frames(
            (await client.get(f"/threads/{_THREAD}/runs/{run_id}/stream")).text
        )
        assert [name for _, name, _ in joined] == ["error"]
        assert joined[0][2]["run_id"] == run_id


# --- outcomes decided after the terminal write -----------------------------------


async def test_superseded_stream_whose_insert_committed_is_not_stranded() -> None:
    # The cancel lands after the row insert committed: the row must still be
    # terminalized, or it would sit ``pending`` until the orphan sweep.
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        create_run = store.create_run
        inserted, never = asyncio.Event(), asyncio.Event()
        rows: list[Any] = []

        async def insert_then_stall(*args: Any, **kwargs: Any) -> Any:
            if inserted.is_set():
                return await create_run(*args, **kwargs)
            rows.append(await create_run(*args, **kwargs))
            inserted.set()
            await never.wait()
            return rows[0]  # pragma: no cover - cancelled before this

        store.create_run = insert_then_stall
        queued = asyncio.create_task(
            ops.create_streaming_run(_THREAD, _request(multitask_strategy="enqueue"))
        )
        await inserted.wait()
        run, events = await asyncio.wait_for(
            ops.create_streaming_run(_THREAD, _request(multitask_strategy="interrupt")),
            5,
        )
        await _drain(events)
        with pytest.raises(HTTPException) as superseded:
            await queued
        assert superseded.value.status_code == 409
        stranded = str(rows[0]["run_id"])
        assert (await ops.get_run(_THREAD, stranded)).status == "interrupted"
        # The superseding run owns the thread status.
        assert (await ops.get_run(_THREAD, str(run.run_id))).status == "success"
        assert not ops._lock_manager.get(_THREAD).locked()


async def test_cancel_after_success_committed_reports_success() -> None:
    async with running_app() as (app, _graph, client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        update_thread = store.update_thread
        settling, never = asyncio.Event(), asyncio.Event()

        async def stall_settle(thread_id: str, **kwargs: Any) -> Any:
            if kwargs.get("mark_state_updated") and not settling.is_set():
                settling.set()  # only the first settle stalls
                await never.wait()
            return await update_thread(thread_id, **kwargs)

        store.update_thread = stall_settle
        run, original = await ops.create_streaming_run(_THREAD, _request())
        run_id = str(run.run_id)
        body = asyncio.create_task(_drain(original))
        await asyncio.wait_for(settling.wait(), 5)
        # ``success`` is committed; now the run is cancelled (a superseding
        # interrupt or shutdown — the cancel endpoint refuses terminal rows).
        assert await ops._registry.cancel(run_id, wait=False)
        frames = parse_frames(await asyncio.wait_for(body, 5))
        store.update_thread = update_thread
        assert frames[-1][1] == "end"
        assert frames[-1][2]["status"] == "success"
        assert (await ops.get_run(_THREAD, run_id)).status == "success"
        # The cancelled settle is redone, so the thread doesn't stay busy.
        thread = (await client.get(f"/threads/{_THREAD}")).json()
        assert thread["status"] == "idle"
        assert ops._unsettled_threads == set()


async def test_failure_after_success_committed_reports_success() -> None:
    async with running_app() as (app, _graph, client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        update_thread = store.update_thread

        async def fail_settle(thread_id: str, **kwargs: Any) -> Any:
            if kwargs.get("mark_state_updated"):
                raise RuntimeError("store blip")
            return await update_thread(thread_id, **kwargs)

        store.update_thread = fail_settle
        run, original = await ops.create_streaming_run(_THREAD, _request())
        frames = parse_frames(await asyncio.wait_for(_drain(original), 5))
        store.update_thread = update_thread
        assert frames[-1][1] == "end"
        assert frames[-1][2]["status"] == "success"
        assert (await ops.get_run(_THREAD, str(run.run_id))).status == "success"
        # The failed settle doesn't rewrite the thread as ``error``; it stays
        # busy only until the next liveness pass retries the settle.
        thread = (await client.get(f"/threads/{_THREAD}")).json()
        assert thread["status"] == "busy"
        assert ops._unsettled_threads == {_THREAD}
        await ops.liveness_pass(stale_after_seconds=None)
        thread = (await client.get(f"/threads/{_THREAD}")).json()
        assert thread["status"] == "idle"
        assert ops._unsettled_threads == set()


async def test_wait_fails_closed_when_run_scoped_history_fails() -> None:
    # /runs/wait and /join read the same final state as a stream join: a
    # fallback to the latest thread state could return a later run's output.
    async with running_app() as (_app, graph, client):

        async def broken_history(*_args: Any, **_kwargs: Any) -> AsyncIterator[Any]:
            raise RuntimeError("history store down")
            yield  # pragma: no cover - makes this an async generator

        graph.aget_state_history = broken_history  # type: ignore[method-assign]
        r = await client.post(
            f"/threads/{_THREAD}/runs/wait",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": []},
                "if_not_exists": "create",
            },
        )
        assert r.status_code == 500
        assert "Could not read the final state" in r.json()["detail"]


async def test_background_run_failing_after_a_sweep_leaves_the_thread_alone() -> None:
    # Another worker swept the run and a newer run made the thread busy; this
    # run's late failure must not rewrite that thread as ``error``.
    async with running_app() as (app, graph, client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        execute = ops._execute_graph_run

        async def run_then_fail(*args: Any, **kwargs: Any) -> Any:
            await execute(*args, **kwargs)
            raise RuntimeError("graph failed late")

        ops._execute_graph_run = run_then_fail
        graph.invoke_gate = asyncio.Event()
        run = await ops.create_run(_THREAD, _request(stream_resumable=False))
        run_id = str(run.run_id)
        await graph.invoke_started.wait()
        await store.update_run_status(run_id, "error", error="orphaned elsewhere")
        await store.update_thread(_THREAD, status_value="busy")
        graph.invoke_gate.set()
        await asyncio.wait_for(ops._registry.wait(run_id), 5)
        row = await store.fetch_run_row(_THREAD, run_id)
        assert row is not None
        assert (row["status"], row["error"]) == ("error", "orphaned elsewhere")
        thread = (await client.get(f"/threads/{_THREAD}")).json()
        assert thread["status"] == "busy"


# --- admission: hand-off to the producer, disconnects while queued -------------


async def test_queued_stream_stays_tracked_through_the_producer_hand_off() -> None:
    # The instant admission completes, before the request resumes, the run
    # must still read as active, or reject/interrupt/rollback would miss it.
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        create_run = store.create_run
        seen: list[set[str]] = []
        graph.stream_gate = asyncio.Event()
        holder, held = await ops.create_streaming_run(_THREAD, _request())
        holder_body = asyncio.create_task(_drain(held))
        await graph.stream_started.wait()

        async def insert_then_check(*args: Any, **kwargs: Any) -> Any:
            row = await create_run(*args, **kwargs)
            # Runs after admission finishes but before its done callbacks.
            asyncio.get_running_loop().call_soon(
                lambda: seen.append(ops._registry.active_runs(_THREAD))
            )
            return row

        store.create_run = insert_then_check
        queued = asyncio.create_task(
            ops.create_streaming_run(_THREAD, _request(multitask_strategy="enqueue"))
        )
        await asyncio.sleep(0.05)
        graph.stream_gate.set()
        run, events = await asyncio.wait_for(queued, 5)
        store.create_run = create_run
        await _drain(events)
        await holder_body
        assert seen == [{str(run.run_id)}]


@pytest.mark.parametrize(
    ("on_disconnect", "expected"), [("continue", "success"), ("cancel", "interrupted")]
)
async def test_client_leaving_a_queued_stream_honors_on_disconnect(
    on_disconnect: str, expected: str
) -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        graph.stream_gate = asyncio.Event()
        _holder, held = await ops.create_streaming_run(_THREAD, _request())
        holder_body = asyncio.create_task(_drain(held))
        await graph.stream_started.wait()
        create_run = store.create_run
        created: list[str] = []

        async def record(*args: Any, **kwargs: Any) -> Any:
            row = await create_run(*args, **kwargs)
            created.append(str(row["run_id"]))
            return row

        store.create_run = record
        queued = asyncio.create_task(
            ops.create_streaming_run(
                _THREAD,
                _request(multitask_strategy="enqueue", on_disconnect=on_disconnect),
            )
        )
        await asyncio.sleep(0.05)
        queued.cancel()  # the client leaves while the run waits for the lock
        with pytest.raises(asyncio.CancelledError):
            await queued
        graph.stream_gate.set()
        await holder_body
        for _ in range(200):
            await asyncio.sleep(0.005)
            if not ops._registry.active_runs(_THREAD):
                break
        store.create_run = create_run
        if expected == "success":
            assert len(created) == 1
            assert (await ops.get_run(_THREAD, created[0])).status == "success"
        else:
            assert created == []  # cancelled before admission inserted a row
        assert not ops._lock_manager.get(_THREAD).locked()


@pytest.mark.parametrize(
    ("on_disconnect", "expected_runs"), [("continue", 1), ("cancel", 0)]
)
async def test_stateless_stream_left_during_admission_discards_its_thread_once(
    on_disconnect: str, expected_runs: int
) -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        entered, release = asyncio.Event(), asyncio.Event()
        create_run = store.create_run
        created: list[tuple[str, str]] = []

        async def stalled(*args: Any, **kwargs: Any) -> Any:
            entered.set()
            await release.wait()
            row = await create_run(*args, **kwargs)
            created.append((str(row["thread_id"]), str(row["run_id"])))
            return row

        discarded: list[str] = []
        discard = ops._discard_thread

        async def count(thread_id: str) -> None:
            discarded.append(thread_id)
            await discard(thread_id)

        store.create_run = stalled
        ops._discard_thread = count
        leaving = asyncio.create_task(
            ops.create_stateless_streaming_run(_request(on_disconnect=on_disconnect))
        )
        await entered.wait()
        leaving.cancel()  # the client leaves while admission inserts the row
        with pytest.raises(asyncio.CancelledError):
            await leaving
        release.set()
        for _ in range(200):
            await asyncio.sleep(0.005)
            if discarded and not ops._registry.active_runs(discarded[0]):
                break
        await asyncio.sleep(0.02)  # a second discard would land here

        assert len(discarded) == 1  # cleanup ran exactly once
        assert discarded[0] not in graph.state_by_thread
        assert len(created) == expected_runs
        if created:
            thread_id, run_id = created[0]
            assert thread_id == discarded[0]
            assert ops._streams._streams.get(run_id) is None or (
                ops._streams._streams[run_id].closed_at is not None
            )


async def test_stateless_stream_failing_before_admission_discards_its_thread_once() -> (
    None
):
    async with running_app() as (app, _graph, _client):
        ops = app.state.skeino.run_ops
        discarded: list[str] = []
        discard = ops._discard_thread

        async def count(thread_id: str) -> None:
            discarded.append(thread_id)
            await discard(thread_id)

        ops._discard_thread = count
        ops._thread_ops.ensure_thread_for_run = AsyncMock(
            side_effect=HTTPException(status_code=503, detail="store down")
        )
        with pytest.raises(HTTPException):
            await ops.create_stateless_streaming_run(_request())
        assert len(discarded) == 1


async def test_stateless_stream_cancelled_before_admission_starts_discards_once() -> (
    None
):
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        discarded: list[str] = []
        discard = ops._discard_thread

        async def count(thread_id: str) -> None:
            discarded.append(thread_id)
            await discard(thread_id)

        spawn = ops._registry.spawn

        def spawn_then_leave(*args: Any, **kwargs: Any) -> Any:
            task = spawn(*args, **kwargs)
            # The client leaves before the admission task's first step, so the
            # cancel lands on a coroutine that never ran.
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            return task

        ops._discard_thread = count
        ops._registry.spawn = spawn_then_leave
        with pytest.raises(asyncio.CancelledError):
            await ops.create_stateless_streaming_run(_request(on_disconnect="cancel"))
        ops._registry.spawn = spawn
        assert len(discarded) == 1
        assert discarded[0] not in graph.state_by_thread
        assert not ops._registry.active_runs(discarded[0])


async def test_admission_failure_after_the_client_left_is_logged() -> None:
    async with running_app() as (app, _graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        entered, release = asyncio.Event(), asyncio.Event()

        async def failing(*args: Any, **kwargs: Any) -> Any:
            entered.set()
            await release.wait()
            raise RuntimeError("insert failed")

        logged: list[str] = []
        log_error = ops._log_error

        def record(msg: str, *args: Any, exc: BaseException | None = None) -> None:
            logged.append(msg % args)
            log_error(msg, *args, exc=exc)

        store.create_run = failing
        ops._log_error = record
        leaving = asyncio.create_task(
            ops.create_streaming_run(_THREAD, _request(on_disconnect="continue"))
        )
        await entered.wait()
        leaving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await leaving
        release.set()
        for _ in range(100):
            await asyncio.sleep(0.005)
            if logged:
                break
        assert any(
            "admission failed after its client left" in line and "insert failed" in line
            for line in logged
        )
        assert not ops._registry.active_runs(_THREAD)
        assert not ops._lock_manager.get(_THREAD).locked()


_SWEPT = "Run orphaned: swept by another worker."


async def test_stream_finalized_before_it_starts_reports_it_and_runs_nothing() -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        create_run = store.create_run

        async def swept_on_insert(*args: Any, **kwargs: Any) -> Any:
            # Another worker sweeps the row before its producer claims it.
            row = await create_run(*args, **kwargs)
            await store.update_run_status(str(row["run_id"]), "error", error=_SWEPT)
            return row

        store.create_run = swept_on_insert
        run, events = await ops.create_streaming_run(_THREAD, _request())
        body = await _drain(events)
        store.create_run = create_run

        assert "event: error" in body and _SWEPT in body
        assert "event: values" not in body
        assert graph.tracing_seen == []  # the graph never ran
        assert (await ops.get_run(_THREAD, str(run.run_id))).status == "error"
        thread = await store.fetch_thread_row(_THREAD)
        assert thread["status"] != "busy"  # the swept run did not take it back


async def test_queued_background_run_finalized_while_waiting_runs_nothing() -> None:
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        graph.invoke_gate = asyncio.Event()
        holder = await ops.create_run(_THREAD, _request())
        await graph.invoke_started.wait()
        queued = await ops.create_run(_THREAD, _request(multitask_strategy="enqueue"))
        queued_id = str(queued.run_id)
        # Swept while it waits for the thread lock.
        await store.update_run_status(queued_id, "error", error=_SWEPT)
        graph.invoke_gate.set()
        await ops.join_run(_THREAD, str(holder.run_id))
        task = ops._registry.get(queued_id)
        if task is not None:
            await asyncio.wait({task})

        assert len(graph.tracing_seen) == 1  # only the holder ran
        swept = await store.fetch_run_row(_THREAD, queued_id)
        assert swept["status"] == "error" and swept["error"] == _SWEPT
        assert (await store.fetch_thread_row(_THREAD))["status"] == "idle"


@pytest.mark.parametrize("streaming", [True, False], ids=["stream", "background"])
async def test_run_whose_row_was_deleted_mid_flight_does_not_report_success(
    streaming: bool,
) -> None:
    # Another worker sweeps the run to ``error`` and the terminal row is then
    # deleted; the original owner finishes late. It must neither report
    # ``success`` for a run that no longer exists nor settle the thread.
    async with running_app() as (app, graph, _client):
        ops = app.state.skeino.run_ops
        store = ops._metadata_store
        gate = asyncio.Event()
        if streaming:
            graph.stream_gate = gate
            run, events = await ops.create_streaming_run(_THREAD, _request())
            body = asyncio.create_task(_drain(events))
            await graph.stream_started.wait()
        else:
            graph.invoke_gate = gate
            run = await ops.create_run(_THREAD, _request())
            await graph.invoke_started.wait()
        run_id = str(run.run_id)
        await store.update_run_status(run_id, "error", error=_SWEPT)
        await store.delete_run(_THREAD, run_id)
        await store.update_thread(_THREAD, status_value="error")  # sweeper's
        task = ops._registry.get(run_id)
        assert task is not None
        gate.set()
        await asyncio.wait({task})

        if streaming:
            text = await body
            assert "was deleted before it finished" in text
            assert '"status":"success"' not in text
        assert (await store.fetch_thread_row(_THREAD))["status"] == "error"
