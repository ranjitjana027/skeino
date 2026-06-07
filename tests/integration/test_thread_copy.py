"""Integration tests for forking/copying a thread (`POST /threads/{id}/copy`)."""

from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient


def _messages_without_ids(values: dict[str, Any]) -> list[dict[str, Any]]:
    """Return message content/type, dropping the serializer's generated ids."""
    return [
        {k: v for k, v in m.items() if k != "id"} for m in values.get("messages", [])
    ]


def _seed_thread_with_state(client: TestClient, content: str) -> str:
    """Create a thread and run once so it has checkpoint state."""
    thread_id = client.post("/threads", json={"metadata": {"topic": "swing"}}).json()[
        "thread_id"
    ]
    client.post(
        f"/threads/{thread_id}/runs",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": content}]},
        },
    )
    return thread_id


def test_copy_creates_independent_thread_with_state(skeino_client: TestClient) -> None:
    source_id = _seed_thread_with_state(skeino_client, "hello")
    source_state = skeino_client.get(f"/threads/{source_id}/state").json()["values"]

    r = skeino_client.post(f"/threads/{source_id}/copy")
    assert r.status_code == 200
    copy = r.json()

    # New, distinct thread with provenance and copied metadata.
    assert copy["thread_id"] != source_id
    assert copy["metadata"]["forked_from"] == source_id
    assert copy["metadata"]["topic"] == "swing"

    # The latest state carried over (ignoring per-call generated message ids).
    copy_state = skeino_client.get(f"/threads/{copy['thread_id']}/state").json()[
        "values"
    ]
    assert copy_state != {}
    assert _messages_without_ids(copy_state) == _messages_without_ids(source_state)


def test_copy_is_isolated_from_source(skeino_client: TestClient) -> None:
    source_id = _seed_thread_with_state(skeino_client, "original")
    copy_id = skeino_client.post(f"/threads/{source_id}/copy").json()["thread_id"]

    # Continue the copy; the source must not change.
    skeino_client.post(
        f"/threads/{copy_id}/runs",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "branch"}]},
        },
    )
    source_after = skeino_client.get(f"/threads/{source_id}/state").json()["values"]
    copy_after = skeino_client.get(f"/threads/{copy_id}/state").json()["values"]
    assert source_after != copy_after


def test_copy_empty_thread(skeino_client: TestClient) -> None:
    source_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.post(f"/threads/{source_id}/copy")
    assert r.status_code == 200
    copy = r.json()
    assert copy["thread_id"] != source_id
    assert copy["metadata"]["forked_from"] == source_id
    assert copy["values"] == {}


def test_copy_missing_thread_returns_404(skeino_client: TestClient) -> None:
    r = skeino_client.post(f"/threads/{uuid4()}/copy")
    assert r.status_code == 404
