"""A run that ends parked on an interrupt leaves its thread ``interrupted``.

A graph that calls ``interrupt()`` finishes its run without error but stops
mid-execution waiting for a human decision. Clients tell the two apart by the
thread's status, so a parked thread must not report ``idle``.
"""

from fastapi.testclient import TestClient
from langgraph.types import Interrupt

from tests.conftest import build_test_app


def _new_thread(client: TestClient) -> str:
    return client.post("/threads", json={}).json()["thread_id"]


def _run(client: TestClient, thread_id: str) -> None:
    response = client.post(
        f"/threads/{thread_id}/runs/wait",
        json={"assistant_id": "test_agent", "input": {"messages": []}},
    )
    assert response.status_code == 200


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
