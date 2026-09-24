"""A run that ends parked on an interrupt says so — in its status and output.

A graph that calls ``interrupt()`` finishes its run without error but stops
mid-execution waiting for a human decision. Clients tell the two apart by the
thread's status and by the ``__interrupt__`` channel in the run output, so a
parked thread must not report ``idle`` and a parked run's output must not look
like a completed one's.
"""

from typing import Any

from fastapi.testclient import TestClient
from langgraph.types import Interrupt

from tests.conftest import build_test_app


def _new_thread(client: TestClient) -> str:
    return client.post("/threads", json={}).json()["thread_id"]


def _run(client: TestClient, thread_id: str) -> Any:
    response = client.post(
        f"/threads/{thread_id}/runs/wait",
        json={"assistant_id": "test_agent", "input": {"messages": []}},
    )
    assert response.status_code == 200
    return response.json()


def test_thread_is_idle_when_nothing_is_pending(skeino_client: TestClient) -> None:
    thread_id = _new_thread(skeino_client)
    _run(skeino_client, thread_id)

    body = skeino_client.get(f"/threads/{thread_id}").json()
    assert body["status"] == "idle"


def test_thread_is_interrupted_while_it_waits_on_a_decision() -> None:
    app, graph = build_test_app()
    graph.pending_interrupts = (
        Interrupt(value={"action_requests": [{"name": "link_broker_account"}]}),
    )
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        _run(client, thread_id)

        body = client.get(f"/threads/{thread_id}").json()
        assert body["status"] == "interrupted"
        assert body["interrupts"]


def test_unreadable_state_falls_back_to_idle() -> None:
    app, graph = build_test_app()

    async def boom(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("checkpoint unavailable")

    with TestClient(app) as client:
        thread_id = _new_thread(client)
        response = client.post(
            f"/threads/{thread_id}/runs/wait",
            json={"assistant_id": "test_agent", "input": {"messages": []}},
        )
        assert response.status_code == 200
        # Break state reads only after the run's own writes are done, then let
        # the next run settle the thread: the run succeeded, so an unreadable
        # checkpoint must not leave the thread claiming a pending decision.
        graph.aget_state = boom  # type: ignore[method-assign]
        client.post(
            f"/threads/{thread_id}/runs/wait",
            json={"assistant_id": "test_agent", "input": {"messages": []}},
        )
        assert client.get(f"/threads/{thread_id}").json()["status"] == "idle"


def test_wait_output_carries_the_pending_interrupt() -> None:
    # The run ends successfully but parked, so its state values alone are
    # indistinguishable from a finished run's: the awaiting tool call sits in
    # ``messages`` with no result after it. LangGraph Platform's ``runs.wait``
    # reports the pause on the reserved ``__interrupt__`` channel, and the
    # streaming path already does — ``wait`` has to as well, or a client on
    # that path can never tell a paused run from a completed one.
    app, graph = build_test_app()
    graph.pending_interrupts = (
        Interrupt(value={"action_requests": [{"name": "link_broker_account"}]}),
    )
    with TestClient(app) as client:
        output = _run(client, _new_thread(client))

    assert isinstance(output, dict)
    # Graph state still comes through — the channel is added, not substituted.
    assert "messages" in output
    interrupts = output["__interrupt__"]
    assert isinstance(interrupts, list) and interrupts
    assert interrupts[0]["value"] == {
        "action_requests": [{"name": "link_broker_account"}]
    }


def test_join_output_carries_the_pending_interrupt() -> None:
    app, graph = build_test_app()
    graph.pending_interrupts = (
        Interrupt(value={"action_requests": [{"name": "link_broker_account"}]}),
    )
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        created = client.post(
            f"/threads/{thread_id}/runs",
            json={"assistant_id": "test_agent", "input": {"messages": []}},
        )
        assert created.status_code == 200
        run_id = created.json()["run_id"]
        joined = client.get(f"/threads/{thread_id}/runs/{run_id}/join")
        assert joined.status_code == 200
        output = joined.json()

    assert isinstance(output, dict)
    interrupts = output["__interrupt__"]
    assert isinstance(interrupts, list) and interrupts
    assert interrupts[0]["value"] == {
        "action_requests": [{"name": "link_broker_account"}]
    }


def test_wait_output_carries_an_interrupt_pending_only_on_a_task() -> None:
    # Older LangGraph snapshots expose a pause per pending task and leave
    # ``snapshot.interrupts`` empty. The output must report it either way.
    app, graph = build_test_app()
    graph.pending_task_interrupts = (Interrupt(value={"question": "approve?"}),)
    with TestClient(app) as client:
        output = _run(client, _new_thread(client))

    assert isinstance(output, dict)
    assert output["__interrupt__"][0]["value"] == {"question": "approve?"}


def test_wait_output_has_no_interrupt_channel_when_nothing_is_pending(
    skeino_client: TestClient,
) -> None:
    # The channel is a signal, not decoration: a completed run must not carry
    # an empty one, or a client reading its truthiness offers a resume that has
    # nothing to resume.
    output = _run(skeino_client, _new_thread(skeino_client))

    assert isinstance(output, dict)
    assert "__interrupt__" not in output
