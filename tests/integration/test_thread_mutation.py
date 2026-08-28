"""Integration tests for thread mutation & time-travel endpoints.

Covers PATCH (metadata), DELETE (thread + checkpoints), POST /state (HITL edit),
and reading state at a specific checkpoint.
"""

from uuid import uuid4

from fastapi.testclient import TestClient

from tests.conftest import build_test_app


def _new_thread(client: TestClient) -> str:
    return client.post("/threads", json={"metadata": {"topic": "swing"}}).json()[
        "thread_id"
    ]


def test_patch_thread_updates_metadata() -> None:
    app, _ = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        r = client.patch(
            f"/threads/{thread_id}", json={"metadata": {"topic": "long_term"}}
        )
        assert r.status_code == 200
        assert r.json()["metadata"]["topic"] == "long_term"
        # Persisted.
        assert (
            client.get(f"/threads/{thread_id}").json()["metadata"]["topic"]
            == "long_term"
        )


def test_patch_thread_empty_body_is_noop() -> None:
    app, _ = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        # An empty PATCH must not wipe existing metadata.
        r = client.patch(f"/threads/{thread_id}", json={})
        assert r.status_code == 200
        assert r.json()["metadata"]["topic"] == "swing"
        # Sending {"metadata": {}} explicitly *does* clear it.
        r = client.patch(f"/threads/{thread_id}", json={"metadata": {}})
        assert r.json()["metadata"] == {}


def test_update_state_preserves_empty_list_payload() -> None:
    app, _ = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        # An empty list payload must reach the graph as a list, not be coerced
        # to {} by truthiness.
        r = client.post(f"/threads/{thread_id}/state", json={"values": []})
        assert r.status_code == 200
        assert client.get(f"/threads/{thread_id}/state").json()["values"] == {
            "data": []
        }


def test_delete_thread_removes_metadata_and_checkpoints() -> None:
    app, graph = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        client.post(
            f"/threads/{thread_id}/runs/wait",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": [{"role": "user", "content": "hi"}]},
            },
        )
        assert thread_id in graph.state_by_thread

        r = client.delete(f"/threads/{thread_id}")
        assert r.status_code == 204

        # Metadata gone (404) and the checkpointer was asked to drop the thread.
        assert client.get(f"/threads/{thread_id}").status_code == 404
        assert thread_id not in graph.state_by_thread


def test_update_state_writes_and_returns_checkpoint() -> None:
    app, _ = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        r = client.post(
            f"/threads/{thread_id}/state",
            json={"values": {"messages": [{"role": "user", "content": "edited"}]}},
        )
        assert r.status_code == 200
        checkpoint = r.json()
        assert checkpoint["thread_id"] == thread_id
        assert checkpoint["checkpoint_id"]  # a new checkpoint was produced

        # The written state is visible.
        state = client.get(f"/threads/{thread_id}/state").json()["values"]
        assert state.get("messages")


def _write_state(client: TestClient, thread_id: str, note: str) -> str:
    """Write a distinct state and return the checkpoint id it produced."""
    r = client.post(f"/threads/{thread_id}/state", json={"values": {"note": note}})
    assert r.status_code == 200
    return r.json()["checkpoint_id"]


def test_get_state_at_checkpoint_id_selects_that_checkpoint() -> None:
    app, _ = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        first = _write_state(client, thread_id, "A")
        second = _write_state(client, thread_id, "B")
        assert first != second

        # Reading the *first* checkpoint returns its state, not the latest.
        r = client.get(f"/threads/{thread_id}/state/{first}")
        assert r.status_code == 200
        body = r.json()
        assert body["checkpoint"]["checkpoint_id"] == first
        assert body["values"]["note"] == "A"
        # Latest still reflects the second write.
        assert client.get(f"/threads/{thread_id}/state").json()["values"]["note"] == "B"


def test_post_state_checkpoint_body_selects_that_checkpoint() -> None:
    app, _ = build_test_app()
    with TestClient(app) as client:
        thread_id = _new_thread(client)
        first = _write_state(client, thread_id, "A")
        _write_state(client, thread_id, "B")

        r = client.post(
            f"/threads/{thread_id}/state/checkpoint",
            json={"thread_id": thread_id, "checkpoint_id": first},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["checkpoint"]["checkpoint_id"] == first
        assert body["values"]["note"] == "A"


def test_mutation_endpoints_404_on_missing_thread() -> None:
    app, _ = build_test_app()
    with TestClient(app) as client:
        missing = str(uuid4())
        assert (
            client.patch(f"/threads/{missing}", json={"metadata": {}}).status_code
            == 404
        )
        assert client.delete(f"/threads/{missing}").status_code == 404
        assert (
            client.post(f"/threads/{missing}/state", json={"values": {}}).status_code
            == 404
        )
