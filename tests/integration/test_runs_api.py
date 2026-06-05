"""Integration tests for the runs API (non-streaming)."""

from fastapi.testclient import TestClient


def test_create_run_succeeds(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.post(
        f"/threads/{thread_id}/runs",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "hello"}]},
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "success"
    assert body["thread_id"] == thread_id
    assert "run_id" in body
    assert "Location" in r.headers


def test_create_run_rejects_after_seconds(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.post(
        f"/threads/{thread_id}/runs",
        json={"assistant_id": "test_agent", "after_seconds": 5.0},
    )
    assert r.status_code == 400


def test_list_runs_returns_recent_run(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    run = skeino_client.post(
        f"/threads/{thread_id}/runs",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "hi"}]},
        },
    ).json()
    r = skeino_client.get(f"/threads/{thread_id}/runs?limit=5")
    assert r.status_code == 200
    rows = r.json()
    assert any(row["run_id"] == run["run_id"] for row in rows)


def test_get_run_returns_one_record(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    run = skeino_client.post(
        f"/threads/{thread_id}/runs",
        json={"assistant_id": "test_agent", "input": {}},
    ).json()
    r = skeino_client.get(f"/threads/{thread_id}/runs/{run['run_id']}")
    assert r.status_code == 200
    assert r.json()["run_id"] == run["run_id"]
