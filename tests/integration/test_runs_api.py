"""Integration tests for the runs API (non-streaming)."""

from fastapi import FastAPI
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage

from tests.conftest import FakeGraph


def test_create_run_returns_background_run(skeino_client: TestClient) -> None:
    # POST /runs is now a background create: it returns immediately with a
    # non-terminal run that subsequently reaches a terminal state.
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
    assert body["status"] in {"pending", "running", "success"}
    assert body["thread_id"] == thread_id
    assert "run_id" in body
    assert "Location" in r.headers
    # The background task settles to success.
    final = skeino_client.get(f"/threads/{thread_id}/runs/{body['run_id']}").json()
    assert final["status"] == "success"


def test_wait_run_returns_output(skeino_client: TestClient) -> None:
    # POST /runs/wait runs to completion and returns the final graph state
    # values (the run output), matching the LangGraph SDK contract.
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.post(
        f"/threads/{thread_id}/runs/wait",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "hello"}]},
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, dict)
    assert any(m.get("content") == "completed" for m in body["messages"])


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


def test_list_runs_accepts_valid_status_filter(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.get(f"/threads/{thread_id}/runs?status=success")
    assert r.status_code == 200


def test_list_runs_rejects_invalid_status_filter(skeino_client: TestClient) -> None:
    # The status query param is now a RunStatus Literal, so FastAPI rejects
    # unknown values at the edge with a 422.
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.get(f"/threads/{thread_id}/runs?status=bogus")
    assert r.status_code == 422


def test_sync_run_reports_callback_usage_in_header(
    skeino_app_and_graph: tuple[FastAPI, FakeGraph], skeino_client: TestClient
) -> None:
    # FakeGraph's checkpoint messages are plain dicts with no usage metadata,
    # so a nonzero header proves the callback handler (not the checkpoint
    # fallback) measured the run.
    _, graph = skeino_app_and_graph
    graph.llm_usage = {"input_tokens": 600, "output_tokens": 400, "total_tokens": 1000}
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    r = skeino_client.post(
        f"/threads/{thread_id}/runs/wait",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "hello"}]},
        },
    )
    assert r.status_code == 200
    assert r.headers["X-Tokens-Used"] == "1000"


def test_sync_run_usage_is_per_run_not_cumulative(
    skeino_app_and_graph: tuple[FastAPI, FakeGraph], skeino_client: TestClient
) -> None:
    # Each run on the same thread must report its own tokens, not the
    # thread's cumulative total.
    _, graph = skeino_app_and_graph
    graph.llm_usage = {"input_tokens": 600, "output_tokens": 400, "total_tokens": 1000}
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    payload = {
        "assistant_id": "test_agent",
        "input": {"messages": [{"role": "user", "content": "hello"}]},
    }
    first = skeino_client.post(f"/threads/{thread_id}/runs/wait", json=payload)
    second = skeino_client.post(f"/threads/{thread_id}/runs/wait", json=payload)
    assert first.headers["X-Tokens-Used"] == "1000"
    assert second.headers["X-Tokens-Used"] == "1000"


def test_sync_run_falls_back_to_checkpoint_usage(
    skeino_app_and_graph: tuple[FastAPI, FakeGraph], skeino_client: TestClient
) -> None:
    # When the callback handler records nothing (llm_usage unset), the header
    # falls back to summing usage over the checkpoint's messages. The run
    # input deliberately has no "messages" key so FakeGraph.ainvoke leaves the
    # pre-seeded usage-bearing message in place.
    _, graph = skeino_app_and_graph
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    graph.state_by_thread[thread_id] = {
        "messages": [
            AIMessage(
                content="prior",
                usage_metadata={
                    "input_tokens": 5,
                    "output_tokens": 5,
                    "total_tokens": 10,
                },
            )
        ]
    }
    r = skeino_client.post(
        f"/threads/{thread_id}/runs/wait",
        json={"assistant_id": "test_agent", "input": {"other": "value"}},
    )
    assert r.status_code == 200
    assert r.headers["X-Tokens-Used"] == "10"
