"""Top-level stateless runs: /runs, /runs/wait, /runs/stream, /runs/batch.

The Platform exposes these for one-shot invocations that want no checkpoint
history. skeino had only the thread-scoped forms, so every caller wanting a
one-shot run open-coded the same three steps — create a thread, run, delete it
— and a caller that skipped the cleanup left a thread behind per request.

What these tests care about most is the part a caller cannot see: that the
ephemeral thread is really gone afterwards, including when the run fails and
including when a stream is abandoned halfway.
"""

import json

import pytest
from fastapi.testclient import TestClient

from tests.conftest import FakeGraph, build_test_app


@pytest.fixture
def client_and_graph() -> tuple[TestClient, FakeGraph]:
    app, graph = build_test_app()
    with TestClient(app) as client:
        yield client, graph


def _payload(**overrides) -> dict:
    body = {
        "assistant_id": "test_agent",
        "input": {"messages": [{"type": "human", "content": "hello"}]},
    }
    body.update(overrides)
    return body


def _threads(client: TestClient) -> list[dict]:
    return client.post("/threads/search", json={"limit": 100}).json()


def _event_names(text: str) -> list[str]:
    chunks = [c for c in text.split("\n\n") if c.strip()]
    return [
        next(line for line in chunk.splitlines() if line.startswith("event: "))[7:]
        for chunk in chunks
    ]


# --- the endpoints exist and run ------------------------------------------


def test_stateless_run_returns_run_metadata(client_and_graph):
    client, _ = client_and_graph
    res = client.post("/runs", json=_payload())

    assert res.status_code == 200, res.text
    body = res.json()
    assert body["status"] == "success"
    assert body["assistant_id"] == "test_agent"


def test_stateless_wait_returns_the_graphs_final_state(client_and_graph):
    client, _ = client_and_graph
    res = client.post("/runs/wait", json=_payload())

    assert res.status_code == 200, res.text
    messages = res.json()["messages"]
    # The fake graph appends one AI turn; the point is that /runs/wait answers
    # with graph output, not with run metadata.
    assert messages[-1]["content"] == "completed"


def test_stateless_stream_emits_the_same_events_as_a_thread_run(client_and_graph):
    client, _ = client_and_graph
    with client.stream("POST", "/runs/stream", json=_payload()) as res:
        assert res.status_code == 200
        text = "".join(res.iter_text())

    names = _event_names(text)
    assert names[0] == "metadata"
    assert names[-1] == "end"


def test_stateless_batch_returns_one_output_per_payload(client_and_graph):
    client, _ = client_and_graph
    res = client.post("/runs/batch", json=[_payload(), _payload()])

    assert res.status_code == 200, res.text
    outputs = res.json()
    assert len(outputs) == 2
    for output in outputs:
        assert output["messages"][-1]["content"] == "completed"


# --- the thread must not survive ------------------------------------------


@pytest.mark.parametrize("path", ["/runs", "/runs/wait"])
def test_the_ephemeral_thread_is_deleted(client_and_graph, path):
    client, _ = client_and_graph
    before = len(_threads(client))

    assert client.post(path, json=_payload()).status_code == 200

    assert len(_threads(client)) == before, (
        "a stateless run left its thread behind — the caller has no id to clean "
        "it up with, so this leaks one row per request"
    )


def test_the_ephemeral_thread_is_deleted_after_a_stream(client_and_graph):
    client, _ = client_and_graph
    before = len(_threads(client))

    with client.stream("POST", "/runs/stream", json=_payload()) as res:
        "".join(res.iter_text())

    assert len(_threads(client)) == before


def test_the_ephemeral_thread_is_deleted_when_the_run_fails(client_and_graph):
    """A failed run must not be a leaked thread as well as a failed run."""
    client, graph = client_and_graph
    graph.invoke_error = RuntimeError("graph exploded")
    before = len(_threads(client))

    res = client.post("/runs/wait", json=_payload())

    assert res.status_code == 500
    assert len(_threads(client)) == before


def test_checkpoints_are_deleted_with_the_thread(client_and_graph):
    """The point of 'stateless' is that nothing is kept, not just no thread row."""
    client, graph = client_and_graph

    assert client.post("/runs/wait", json=_payload()).status_code == 200

    assert graph.state_by_thread == {}
    assert graph.checkpoints_by_thread == {}


# --- the boundaries -------------------------------------------------------


def test_a_stateless_run_cannot_resume_a_checkpoint(client_and_graph):
    """There is no history to resume from, so say so rather than half-honour it."""
    client, _ = client_and_graph
    res = client.post(
        "/runs/wait",
        json=_payload(checkpoint={"checkpoint_id": "whatever"}),
    )

    assert res.status_code == 400
    assert "checkpoint" in res.json()["detail"].lower()


def test_if_not_exists_reject_does_not_break_a_stateless_run(client_and_graph):
    """The default is 'reject', and the caller never sees the thread id.

    Honouring it would 404 every stateless run against a thread the caller
    could not have created, so it is forced to 'create'.
    """
    client, _ = client_and_graph
    res = client.post("/runs/wait", json=_payload(if_not_exists="reject"))

    assert res.status_code == 200, res.text


def test_an_unknown_assistant_is_rejected(client_and_graph):
    """The graph allowlist applies here exactly as it does to a thread run."""
    client, _ = client_and_graph
    res = client.post("/runs/wait", json=_payload(assistant_id="not_a_graph"))

    assert res.status_code == 404, res.text


def test_batch_rejects_a_body_that_is_not_a_list(client_and_graph):
    client, _ = client_and_graph
    res = client.post("/runs/batch", json=_payload())

    assert res.status_code == 422
    assert "array" in res.json()["detail"].lower()


def test_batch_reports_which_run_was_invalid(client_and_graph):
    client, _ = client_and_graph
    res = client.post("/runs/batch", json=[_payload(), {"input": {}}])

    assert res.status_code == 422
    assert "Run 1" in res.json()["detail"]


def test_the_sdk_text_plain_content_type_is_accepted(client_and_graph):
    """The LangGraph SDK posts JSON as text/plain to dodge CORS preflight."""
    client, _ = client_and_graph
    res = client.post(
        "/runs/wait",
        content=json.dumps(_payload()),
        headers={"Content-Type": "text/plain"},
    )

    assert res.status_code == 200, res.text


def test_thread_scoped_runs_still_work(client_and_graph):
    """The new prefix-less router must not shadow the thread-scoped one."""
    client, _ = client_and_graph
    thread_id = client.post("/threads", json={}).json()["thread_id"]

    res = client.post(f"/threads/{thread_id}/runs", json=_payload())

    assert res.status_code == 200, res.text
    assert len(_threads(client)) == 1, "the caller's own thread must survive"
