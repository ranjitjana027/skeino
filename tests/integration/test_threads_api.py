"""Integration tests for the threads API."""

from uuid import uuid4

from fastapi.testclient import TestClient


def test_create_thread_returns_idle(skeino_client: TestClient) -> None:
    r = skeino_client.post("/threads", json={"metadata": {"topic": "swing"}})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "idle"
    assert body["metadata"]["topic"] == "swing"
    assert "thread_id" in body


def test_get_thread_state_returns_empty_values(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.get(f"/threads/{thread_id}/state")
    assert r.status_code == 200
    state = r.json()
    assert state["checkpoint"]["thread_id"] == thread_id
    assert state["values"] == {}


def test_search_threads_filters_by_metadata(skeino_client: TestClient) -> None:
    first = skeino_client.post("/threads", json={"metadata": {"topic": "swing"}}).json()
    skeino_client.post("/threads", json={"metadata": {"topic": "long_term"}}).json()
    r = skeino_client.post("/threads/search", json={"metadata": {"topic": "swing"}})
    assert r.status_code == 200
    results = r.json()
    assert [t["thread_id"] for t in results] == [first["thread_id"]]


def test_get_missing_thread_returns_404(skeino_client: TestClient) -> None:
    missing = str(uuid4())
    r = skeino_client.get(f"/threads/{missing}")
    assert r.status_code == 404


def test_history_records_prior_states(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    skeino_client.post(
        f"/threads/{thread_id}/runs",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "hi"}]},
        },
    )
    r = skeino_client.get(f"/threads/{thread_id}/history?limit=5")
    assert r.status_code == 200
    history = r.json()
    assert len(history) >= 1
    assert history[0]["checkpoint"]["thread_id"] == thread_id
