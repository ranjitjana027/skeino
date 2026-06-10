"""Integration tests for the streaming runs endpoint."""

import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.conftest import FakeGraph


def _stream_chunks(response) -> list[str]:
    """Split an SSE response body into discrete event chunks."""
    body = response.text
    return [chunk for chunk in body.split("\n\n") if chunk.strip()]


def test_stream_emits_metadata_values_and_end(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    with skeino_client.stream(
        "POST",
        f"/threads/{thread_id}/runs/stream",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "stream this"}]},
            "stream_mode": ["updates", "values"],
        },
    ) as r:
        assert r.status_code == 200
        text = "".join(r.iter_text())
    chunks = [c for c in text.split("\n\n") if c.strip()]
    events = [
        next(line for line in chunk.splitlines() if line.startswith("event: "))[7:]
        for chunk in chunks
    ]
    assert events[0] == "metadata"
    assert "updates" in events
    assert "values" in events
    assert events[-1] == "end"


def test_stream_metadata_includes_thread_and_run_ids(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    with skeino_client.stream(
        "POST",
        f"/threads/{thread_id}/runs/stream",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "hi"}]},
            "stream_mode": "values",
        },
    ) as r:
        text = "".join(r.iter_text())
    metadata_chunk = next(
        c for c in text.split("\n\n") if c.strip() and "event: metadata" in c
    )
    data_line = next(
        line for line in metadata_chunk.splitlines() if line.startswith("data: ")
    )
    payload = json.loads(data_line[6:])
    assert payload["thread_id"] == thread_id
    assert "run_id" in payload


def test_stream_rejects_events_with_extra_modes(skeino_client: TestClient) -> None:
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    with skeino_client.stream(
        "POST",
        f"/threads/{thread_id}/runs/stream",
        json={
            "assistant_id": "test_agent",
            "stream_mode": ["events", "values"],
        },
    ) as r:
        # Status 400 before any event is emitted
        assert r.status_code == 400


def test_stream_end_event_reports_callback_usage(
    skeino_app_and_graph: tuple[FastAPI, FakeGraph], skeino_client: TestClient
) -> None:
    # The end event's usage must come from the per-run callback handler:
    # FakeGraph's checkpoint messages are plain dicts with no usage metadata,
    # so the old checkpoint summation would report 0 here.
    _, graph = skeino_app_and_graph
    graph.llm_usage = {"input_tokens": 600, "output_tokens": 400, "total_tokens": 1000}
    thread_id = skeino_client.post("/threads", json={}).json()["thread_id"]
    with skeino_client.stream(
        "POST",
        f"/threads/{thread_id}/runs/stream",
        json={
            "assistant_id": "test_agent",
            "input": {"messages": [{"role": "user", "content": "stream this"}]},
            "stream_mode": "values",
        },
    ) as r:
        assert r.status_code == 200
        text = "".join(r.iter_text())
    end_chunk = next(c for c in text.split("\n\n") if c.strip() and "event: end" in c)
    data_line = next(
        line for line in end_chunk.splitlines() if line.startswith("data: ")
    )
    payload = json.loads(data_line[6:])
    assert payload["usage"]["total_tokens"] == 1000
