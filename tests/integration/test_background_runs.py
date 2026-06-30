"""Background run lifecycle: create, wait, join, cancel, delete, multitask.

These drive ``RunOps`` directly inside the app's lifespan (one event loop, like
the streaming-cancellation test) so a gated long-running run can be observed
``running`` and then cancelled, joined, or superseded. ``FakeGraph.invoke_gate``
parks ``ainvoke`` until released, making the in-flight window deterministic.
"""

import asyncio

import pytest
from fastapi import HTTPException

from skeino.schemas import RunCreateRequest
from tests.conftest import FakeGraph, build_test_app

_THREAD = "11111111-1111-1111-1111-111111111111"


def _req(strategy: str = "enqueue", *, text: str | None = None) -> RunCreateRequest:
    messages = [{"role": "user", "content": text}] if text is not None else []
    return RunCreateRequest(
        assistant_id="test_agent",
        input={"messages": messages},
        if_not_exists="create",
        multitask_strategy=strategy,  # type: ignore[arg-type]
    )


async def test_background_create_returns_pending_then_succeeds() -> None:
    app, _ = build_test_app()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        run = await run_ops.create_run(_THREAD, _req())
        assert run.status in {"pending", "running"}
        # join waits for the background task to finish.
        await run_ops.join_run(_THREAD, str(run.run_id))
        final = await run_ops.get_run(_THREAD, str(run.run_id))
        assert final.status == "success"


async def test_wait_run_returns_output_values() -> None:
    app, _ = build_test_app()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        output, _tokens = await run_ops.wait_run(
            _THREAD,
            RunCreateRequest(
                assistant_id="test_agent",
                input={"messages": [{"role": "user", "content": "hi"}]},
                if_not_exists="create",
            ),
        )
        assert isinstance(output, dict)
        assert any(m.get("content") == "completed" for m in output["messages"])


async def test_join_unknown_run_is_404() -> None:
    app, _ = build_test_app()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        await run_ops._thread_ops.ensure_thread_for_run(_THREAD, "create")
        with pytest.raises(HTTPException) as exc:
            await run_ops.join_run(_THREAD, "22222222-2222-2222-2222-222222222222")
        assert exc.value.status_code == 404


async def test_cancel_interrupt_marks_interrupted_and_releases_lock() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        run = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()  # task parked mid-run

        running = await run_ops.get_run(_THREAD, str(run.run_id))
        assert running.status == "running"

        await run_ops.cancel_run(
            _THREAD, str(run.run_id), action="interrupt", wait=True
        )
        cancelled = await run_ops.get_run(_THREAD, str(run.run_id))
        assert cancelled.status == "interrupted"
        assert not run_ops._lock_manager.get(_THREAD).locked()


async def test_cancel_interrupt_wait_false_defers_status_to_the_task() -> None:
    # With wait=False the run must NOT be marked terminal eagerly — the task is
    # still unwinding (and may still hold the lock). Its own CancelledError
    # handler persists ``interrupted`` once it exits.
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        run = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()
        run_id = str(run.run_id)

        await run_ops.cancel_run(_THREAD, run_id, action="interrupt", wait=False)
        task = run_ops._registry.get(run_id)
        assert task is not None and not task.done()  # still unwinding
        assert (await run_ops.get_run(_THREAD, run_id)).status != "interrupted"

        await asyncio.wait({task})  # let the task's handler run
        assert (await run_ops.get_run(_THREAD, run_id)).status == "interrupted"


async def test_cancel_rollback_deletes_the_run() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        run = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()

        await run_ops.cancel_run(_THREAD, str(run.run_id), action="rollback", wait=True)
        with pytest.raises(HTTPException) as exc:
            await run_ops.get_run(_THREAD, str(run.run_id))
        assert exc.value.status_code == 404


async def test_cancel_rollback_waits_for_unwind_even_when_wait_false() -> None:
    # rollback must let the task fully unwind before deleting, regardless of the
    # wait flag. Proof: the execution lock (released only in the task's finally)
    # is free by the time cancel_run returns, and the row is gone.
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        run = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()

        await run_ops.cancel_run(
            _THREAD, str(run.run_id), action="rollback", wait=False
        )
        assert not run_ops._lock_manager.get(_THREAD).locked()
        with pytest.raises(HTTPException) as exc:
            await run_ops.get_run(_THREAD, str(run.run_id))
        assert exc.value.status_code == 404


async def test_cancel_terminal_run_is_409() -> None:
    app, _ = build_test_app()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        run = await run_ops.create_run(_THREAD, _req())
        await run_ops.join_run(_THREAD, str(run.run_id))
        with pytest.raises(HTTPException) as exc:
            await run_ops.cancel_run(
                _THREAD, str(run.run_id), action="interrupt", wait=True
            )
        assert exc.value.status_code == 409


async def test_delete_run_requires_terminal_state() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        run = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()

        with pytest.raises(HTTPException) as exc:
            await run_ops.delete_run(_THREAD, str(run.run_id))
        assert exc.value.status_code == 409

        graph.invoke_gate.set()  # let it finish
        await run_ops.join_run(_THREAD, str(run.run_id))
        await run_ops.delete_run(_THREAD, str(run.run_id))
        with pytest.raises(HTTPException) as gone:
            await run_ops.get_run(_THREAD, str(run.run_id))
        assert gone.value.status_code == 404


async def test_multitask_reject_409_when_busy() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()
        with pytest.raises(HTTPException) as exc:
            await run_ops.create_run(_THREAD, _req("reject"))
        assert exc.value.status_code == 409


async def test_multitask_interrupt_cancels_active_run() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        first = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()
        graph.invoke_started.clear()

        await run_ops.create_run(_THREAD, _req("interrupt"))
        superseded = await run_ops.get_run(_THREAD, str(first.run_id))
        assert superseded.status == "interrupted"


async def test_multitask_rollback_deletes_active_run() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        first = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()
        graph.invoke_started.clear()

        await run_ops.create_run(_THREAD, _req("rollback"))
        with pytest.raises(HTTPException) as exc:
            await run_ops.get_run(_THREAD, str(first.run_id))
        assert exc.value.status_code == 404


async def test_multitask_enqueue_runs_sequentially() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        first = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()
        second = await run_ops.create_run(_THREAD, _req())  # enqueue (default)

        # The second run is queued behind the first on the execution lock.
        graph.invoke_gate.set()
        await run_ops.join_run(_THREAD, str(first.run_id))
        await run_ops.join_run(_THREAD, str(second.run_id))
        assert (await run_ops.get_run(_THREAD, str(first.run_id))).status == "success"
        assert (await run_ops.get_run(_THREAD, str(second.run_id))).status == "success"


async def test_join_returns_run_scoped_output_not_the_next_run() -> None:
    # With an enqueued follow-on run, join/wait must return the *requested*
    # run's output, not whatever the next run left as the latest thread state.
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        first = await run_ops.create_run(_THREAD, _req(text="alpha"))
        await graph.invoke_started.wait()
        graph.invoke_started.clear()
        second = await run_ops.create_run(_THREAD, _req(text="beta"))  # enqueued

        graph.invoke_gate.set()  # first finishes, then second runs
        out_first = await run_ops.join_run(_THREAD, str(first.run_id))
        out_second = await run_ops.join_run(_THREAD, str(second.run_id))

        assert isinstance(out_first, dict) and isinstance(out_second, dict)
        assert any(m.get("content") == "alpha" for m in out_first["messages"])
        assert any(m.get("content") == "beta" for m in out_second["messages"])


async def test_shutdown_marks_queued_run_interrupted() -> None:
    # A run cancelled while still *queued* for the execution lock (never started)
    # must be persisted as ``interrupted``, not left stuck at ``pending``.
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        running = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()  # first holds the lock, parked
        queued = await run_ops.create_run(_THREAD, _req())  # waits on the lock
    # Lifespan exit cancels both; the queued one never acquired the lock.
    assert (await run_ops.get_run(_THREAD, str(running.run_id))).status == "interrupted"
    assert (await run_ops.get_run(_THREAD, str(queued.run_id))).status == "interrupted"


async def test_join_live_streaming_run_is_409() -> None:
    # A live streaming run is registered as external (no joinable task); joining
    # it must fail fast rather than return a non-terminal snapshot.
    app, _ = build_test_app()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        request = RunCreateRequest(
            assistant_id="test_agent",
            input={"messages": []},
            if_not_exists="create",
            stream_mode="values",
        )
        run, stream = await run_ops.create_streaming_run(_THREAD, request)
        await stream.__anext__()  # start the generator (emit metadata)
        try:
            with pytest.raises(HTTPException) as exc:
                await run_ops.join_run(_THREAD, str(run.run_id))
            assert exc.value.status_code == 409
        finally:
            await stream.aclose()  # runs the generator's finally → releases lock


async def test_join_returns_404_when_run_deleted_concurrently() -> None:
    # If the run row is removed (rollback / DELETE) before output is collected,
    # the waiter sees a 404, not a misleading 500.
    app, _ = build_test_app()
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        run = await run_ops.create_run(_THREAD, _req())
        await run_ops.join_run(_THREAD, str(run.run_id))  # reach terminal
        await run_ops._metadata_store.delete_run(_THREAD, str(run.run_id))
        with pytest.raises(HTTPException) as exc:
            await run_ops._collect_terminal_output(_THREAD, str(run.run_id), None)
        assert exc.value.status_code == 404


async def test_shutdown_cancels_in_flight_runs() -> None:
    app, graph = build_test_app()
    assert isinstance(graph, FakeGraph)
    async with app.router.lifespan_context(app):
        run_ops = app.state.skeino.run_ops
        graph.invoke_gate = asyncio.Event()
        run = await run_ops.create_run(_THREAD, _req())
        await graph.invoke_started.wait()
    # Lifespan exit cancelled the background task via run_ops.shutdown().
    final = await run_ops.get_run(_THREAD, str(run.run_id))
    assert final.status == "interrupted"
