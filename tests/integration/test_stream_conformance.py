"""Streaming conformance against real LangGraph graphs.

The contract is the upstream LangGraph server's SSE protocol (what
``langgraph-sdk`` clients parse), checked mode by mode with graphs from
``tests/real_graphs.py``, so chunk shapes are exactly what LangGraph emits:

* every stream opens with ``metadata`` and closes with ``end``;
* each requested mode yields events named after it (``messages-tuple`` is
  delivered as ``messages``; subgraph events as ``mode|namespace``);
* payloads have the mode's shape; ``values``/``updates`` honour the graph's
  ``output_schema`` while ``__interrupt__`` always passes through.

Known gaps are strict xfails tied to their issues (#113, #123, #124).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.real_graphs import (
    ASSISTANT_ID,
    FAKE_LLM_REPLY,
    parse_sse,
    real_client,
    user_input,
)

Events = list[tuple[str, Any]]

XFAIL_113 = pytest.mark.xfail(
    strict=True, reason="#113: messages / messages-tuple streaming broken"
)
XFAIL_123 = pytest.mark.xfail(
    strict=True, reason="#123: subgraph event names are tuple reprs"
)
XFAIL_124 = pytest.mark.xfail(
    strict=True, reason="#124: interrupt invisible without updates/values"
)

# Event names each requested mode may produce (upstream langgraph_api/stream.py).
MODE_EVENTS: dict[str, set[str]] = {
    "values": {"values"},
    "updates": {"updates"},
    "messages": {"messages/metadata", "messages/partial", "messages/complete"},
    "messages-tuple": {"messages"},
    "custom": {"custom"},
    "events": {"events"},
    "debug": {"debug"},
    "tasks": {"tasks"},
    "checkpoints": {"checkpoints"},
}
ALL_MODES = list(MODE_EVENTS)


def _stream(
    client: TestClient, stream_mode: list[str], *, path: str | None = None, **body: Any
) -> Events:
    if path is None:
        thread_id = client.post("/threads", json={}).json()["thread_id"]
        path = f"/threads/{thread_id}/runs/stream"
    payload = {"input": user_input(), **body}
    response = client.post(
        path,
        json={"assistant_id": ASSISTANT_ID, "stream_mode": stream_mode, **payload},
    )
    assert response.status_code == 200, response.text
    return parse_sse(response.text)


def _names(events: Events) -> list[str]:
    return [name for name, _ in events]


def _data(events: Events, name: str) -> list[Any]:
    return [data for event, data in events if event == name]


def _mode_params(broken: set[str], mark: pytest.MarkDecorator) -> list[Any]:
    return [
        pytest.param(mode, marks=mark) if mode in broken else mode for mode in ALL_MODES
    ]


# --- framing ---------------------------------------------------------------


@pytest.mark.parametrize("mode", ALL_MODES)
def test_stream_is_framed_by_metadata_and_end(mode: str) -> None:
    with real_client("fake_llm") as client:
        events = _stream(client, [mode])
    names = _names(events)
    assert names[0] == "metadata"
    assert names[-1] == "end"
    assert names.count("metadata") == names.count("end") == 1
    assert events[-1][1]["status"] == "success"


@pytest.mark.parametrize(
    "mode", _mode_params({"messages", "messages-tuple"}, XFAIL_113)
)
def test_requested_mode_produces_only_its_own_events(mode: str) -> None:
    # ``custom`` needs a graph that writes to the stream; the rest use an LLM node.
    with real_client("writer" if mode == "custom" else "fake_llm") as client:
        events = _stream(client, [mode])
    body = set(_names(events)[1:-1])
    assert body, f"{mode} produced no events"
    assert body <= MODE_EVENTS[mode], body - MODE_EVENTS[mode]


def test_multiple_modes_each_produce_their_events() -> None:
    with real_client("writer") as client:
        events = _stream(client, ["updates", "values", "custom"])
    assert {"updates", "values", "custom"} <= set(_names(events))


# --- per-mode payload shape ------------------------------------------------


def test_values_events_are_full_state_snapshots() -> None:
    with real_client("echo") as client:
        snapshots = _data(_stream(client, ["values"]), "values")
    assert [len(s["messages"]) for s in snapshots] == [1, 2]
    assert snapshots[-1]["messages"][-1]["content"] == "echo: hi"


def test_updates_events_are_per_node_deltas() -> None:
    with real_client("echo") as client:
        updates = _data(_stream(client, ["updates"]), "updates")
    assert updates == [{"reply": {"messages": [updates[0]["reply"]["messages"][0]]}}]
    assert updates[0]["reply"]["messages"][0]["content"] == "echo: hi"


def test_events_mode_proxies_astream_events_v2() -> None:
    with real_client("fake_llm") as client:
        events = _data(_stream(client, ["events"]), "events")
    kinds = {event["event"] for event in events}
    assert {"on_chain_start", "on_chain_end", "on_chat_model_stream"} <= kinds


def test_debug_tasks_and_checkpoints_carry_their_shapes() -> None:
    with real_client("echo") as client:
        events = _stream(client, ["debug", "tasks", "checkpoints"])
    assert {d["type"] for d in _data(events, "debug")} >= {"checkpoint", "task"}
    tasks = _data(events, "tasks")
    assert any(t.get("name") == "reply" and "input" in t for t in tasks)
    assert any(t.get("name") == "reply" and "result" in t for t in tasks)
    assert all({"config", "values"} <= set(c) for c in _data(events, "checkpoints"))


def test_custom_dict_payload_is_forwarded() -> None:
    with real_client("writer") as client:
        assert _data(_stream(client, ["custom"]), "custom") == [{"progress": 50}]


@XFAIL_113
def test_custom_non_dict_payload_is_forwarded() -> None:
    with real_client("writer") as client:
        custom = _data(_stream(client, ["custom"]), "custom")
    assert custom == [{"progress": 50}, "plain-text-progress"]


# --- message streaming (#113) ----------------------------------------------


@XFAIL_113
def test_messages_tuple_streams_message_chunks_with_metadata() -> None:
    with real_client("fake_llm") as client:
        chunks = _data(_stream(client, ["messages-tuple"]), "messages")
    assert chunks
    for chunk in chunks:
        message, metadata = chunk
        assert message["type"] in {"AIMessageChunk", "ai"}
        assert metadata["langgraph_node"] == "call_model"
    assert "".join(m["content"] for m, _ in chunks) == FAKE_LLM_REPLY


@XFAIL_113
def test_messages_mode_streams_partial_messages() -> None:
    with real_client("fake_llm") as client:
        events = _stream(client, ["messages"])
    partials = _data(events, "messages/partial")
    assert _data(events, "messages/metadata")
    assert partials[-1][0]["content"] == FAKE_LLM_REPLY


@XFAIL_113
def test_sdk_default_modes_stream_values_messages_and_custom() -> None:
    """``langgraph-sdk`` / agent-chat-ui default: values + messages-tuple + custom."""
    with real_client("fake_llm") as client:
        names = set(_names(_stream(client, ["values", "messages-tuple", "custom"])))
    assert {"values", "messages"} <= names


# --- output-schema filtering -----------------------------------------------


@pytest.mark.parametrize("graph_name", ["typed_output", "pydantic_output"])
@pytest.mark.parametrize("mode", ["values", "updates"])
def test_state_events_hide_keys_outside_output_schema(
    graph_name: str, mode: str
) -> None:
    with real_client(graph_name) as client:
        payloads = _data(_stream(client, [mode]), mode)
    assert payloads
    assert "internal" not in str(payloads)
    assert "echo: hi" in str(payloads)


@pytest.mark.parametrize("mode", ["values", "updates"])
def test_interrupt_passes_through_state_events(mode: str) -> None:
    with real_client("interrupting") as client:
        events = _stream(client, [mode])
        thread = client.get(f"/threads/{events[0][1]['thread_id']}").json()
    interrupts = [
        d["__interrupt__"] for d in _data(events, mode) if "__interrupt__" in d
    ]
    # Assert the contract fields only: LangGraph adds fields to Interrupt over
    # time (e.g. ``response_schema`` in 1.2.12).
    assert len(interrupts) == 1 and len(interrupts[0]) == 1
    assert interrupts[0][0]["value"] == "approve?"
    assert interrupts[0][0]["id"]
    # Upstream semantics: pausing on interrupt() is a *successful* run; the
    # pause lives on the thread. Run status ``interrupted`` means cancelled.
    assert events[-1][1]["status"] == "success"
    assert thread["status"] == "interrupted"


@XFAIL_124
@pytest.mark.parametrize("mode", ["messages-tuple", "custom"])
def test_interrupt_is_visible_without_state_modes(mode: str) -> None:
    with real_client("interrupting") as client:
        events = _stream(client, [mode])
    updates = _data(events, "updates")
    assert any("__interrupt__" in u for u in updates)


# --- subgraphs (#123) ------------------------------------------------------


def test_subgraph_updates_are_streamed() -> None:
    with real_client("with_subgraph") as client:
        events = _stream(client, ["updates"], stream_subgraphs=True)
    assert len(events) == 4  # metadata, inner update, parent update, end
    assert "inner saw: hi" in str(events)


@XFAIL_123
def test_subgraph_events_are_named_mode_pipe_namespace() -> None:
    with real_client("with_subgraph") as client:
        names = _names(_stream(client, ["updates"], stream_subgraphs=True))[1:-1]
    assert "updates" in names
    assert any(n.startswith("updates|inner:") for n in names)


# --- endpoint parity -------------------------------------------------------


def test_stateless_stream_matches_thread_stream() -> None:
    with real_client("echo") as client:
        threaded = _stream(client, ["values", "updates"])
        stateless = _stream(client, ["values", "updates"], path="/runs/stream")
    assert _names(stateless) == _names(threaded)
    assert _without_ids(stateless[1:-1]) == _without_ids(threaded[1:-1])


def _without_ids(value: Any) -> Any:
    """Drop per-run ``id`` fields so two runs of one graph compare equal."""
    if isinstance(value, dict):
        return {k: _without_ids(v) for k, v in value.items() if k != "id"}
    if isinstance(value, (list, tuple)):
        return [_without_ids(v) for v in value]
    return value
