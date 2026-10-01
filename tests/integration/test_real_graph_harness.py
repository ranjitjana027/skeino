"""Smoke tests for the real-graph harness (``tests/real_graphs.py``).

One behaviour check per graph proves the fixture really exercises LangGraph
through skeino's HTTP surface. Known bugs the harness surfaced are pinned as
``xfail(strict=True)`` with their issue number, so a fix must flip them.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.real_graphs import (
    ASSISTANT_ID,
    FAKE_LLM_REPLY,
    INTERNAL_VALUE,
    parse_sse,
    real_client,
    user_input,
)


def _new_thread(client: TestClient) -> str:
    return str(client.post("/threads", json={}).json()["thread_id"])


def _wait(client: TestClient, thread_id: str, **body: Any) -> Any:
    response = client.post(
        f"/threads/{thread_id}/runs/wait",
        json={"assistant_id": ASSISTANT_ID, **body},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _last_content(values: dict[str, Any]) -> Any:
    return values["messages"][-1]["content"]


def test_echo_graph_replies_and_persists_state() -> None:
    with real_client("echo") as client:
        thread_id = _new_thread(client)
        output = _wait(client, thread_id, input=user_input("ping"))
        assert _last_content(output) == "echo: ping"
        thread = client.get(f"/threads/{thread_id}").json()
        assert thread["status"] == "idle"
        assert [m["type"] for m in thread["values"]["messages"]] == ["human", "ai"]


@pytest.mark.parametrize("graph_name", ["typed_output", "pydantic_output"])
def test_output_schema_hides_internal_key_on_thread_read(graph_name: str) -> None:
    with real_client(graph_name) as client:
        thread_id = _new_thread(client)
        _wait(client, thread_id, input=user_input())
        values = client.get(f"/threads/{thread_id}").json()["values"]
        assert _last_content(values) == "echo: hi"
        assert "internal" not in values


@pytest.mark.parametrize("graph_name", ["typed_output", "pydantic_output"])
def test_output_schema_hides_internal_key_on_values_stream(graph_name: str) -> None:
    with real_client(graph_name) as client:
        thread_id = _new_thread(client)
        body = client.post(
            f"/threads/{thread_id}/runs/stream",
            json={
                "assistant_id": ASSISTANT_ID,
                "input": user_input(),
                "stream_mode": ["values"],
            },
        ).text
        snapshots = [data for name, data in parse_sse(body) if name == "values"]
        assert snapshots
        assert all("internal" not in snapshot for snapshot in snapshots)


@pytest.mark.xfail(strict=True, reason="#120: runs/wait ignores output_schema")
@pytest.mark.parametrize("graph_name", ["typed_output", "pydantic_output"])
def test_output_schema_hides_internal_key_on_wait(graph_name: str) -> None:
    with real_client(graph_name) as client:
        output = _wait(client, _new_thread(client), input=user_input())
        assert INTERNAL_VALUE not in str(output)


def test_writer_graph_streams_dict_custom_event() -> None:
    with real_client("writer") as client:
        body = client.post(
            f"/threads/{_new_thread(client)}/runs/stream",
            json={
                "assistant_id": ASSISTANT_ID,
                "input": user_input(),
                "stream_mode": ["custom"],
            },
        ).text
        custom = [data for name, data in parse_sse(body) if name == "custom"]
        assert {"progress": 50} in custom


@pytest.mark.xfail(strict=True, reason="#113: non-dict stream payloads dropped")
def test_writer_graph_streams_non_dict_custom_event() -> None:
    with real_client("writer") as client:
        body = client.post(
            f"/threads/{_new_thread(client)}/runs/stream",
            json={
                "assistant_id": ASSISTANT_ID,
                "input": user_input(),
                "stream_mode": ["custom"],
            },
        ).text
        custom = [data for name, data in parse_sse(body) if name == "custom"]
        assert "plain-text-progress" in custom


def test_interrupting_graph_pauses_then_resumes() -> None:
    with real_client("interrupting") as client:
        thread_id = _new_thread(client)
        _wait(client, thread_id, input=user_input())
        paused = client.get(f"/threads/{thread_id}").json()
        assert paused["status"] == "interrupted"
        assert len(paused["interrupts"]) == 1

        output = _wait(client, thread_id, command={"resume": "yes"})
        assert _last_content(output) == "approved: yes"
        assert client.get(f"/threads/{thread_id}").json()["status"] == "idle"


@pytest.mark.xfail(strict=True, reason="#121: non-dict interrupt value wrapped")
def test_interrupt_value_is_passed_through_unwrapped() -> None:
    with real_client("interrupting") as client:
        thread_id = _new_thread(client)
        _wait(client, thread_id, input=user_input())
        interrupts = client.get(f"/threads/{thread_id}").json()["interrupts"]
        assert interrupts[0]["value"] == "approve?"


def test_subgraph_output_reaches_parent_state() -> None:
    with real_client("with_subgraph") as client:
        output = _wait(client, _new_thread(client), input=user_input("deep"))
        assert output["inner_result"] == "inner saw: deep"


def test_fake_llm_graph_returns_model_reply() -> None:
    with real_client("fake_llm") as client:
        output = _wait(client, _new_thread(client), input=user_input())
        assert _last_content(output) == FAKE_LLM_REPLY
