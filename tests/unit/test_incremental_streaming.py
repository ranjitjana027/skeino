"""Token-by-token ``values`` streaming and its dispatch trigger.

Real langgraph-sdk clients request ``["values", "messages-tuple", "custom"]``,
so the incremental accumulator must engage on *membership* of ``values`` (not an
exact ``== ["values"]`` match) and must forward ``custom`` (UI) events so
generative-UI consumers keep working alongside token streaming.
"""

from types import SimpleNamespace
from typing import Any

from skeino.streaming.incremental import stream_incremental_values
from skeino.streaming.runner import Streamer


class _Msg:
    """Minimal message chunk exposing ``content`` (+ optional ``tool_calls``)."""

    def __init__(self, content: str) -> None:
        self.content = content
        self.tool_calls: list[dict[str, Any]] = []


class _FakeGraph:
    """Fake compiled graph whose ``astream`` replays a fixed event sequence."""

    output_schema = None  # no output filtering for these tests

    def __init__(self, events: list[tuple[str, Any]]) -> None:
        self._events = events
        self.stream_mode_seen: Any = None

    async def astream(
        self,
        input_value: Any,
        config: dict[str, Any],
        *,
        context: Any = None,
        stream_mode: Any = None,
        interrupt_before: Any = None,
        interrupt_after: Any = None,
        durability: Any = None,
        subgraphs: bool = False,
    ):
        self.stream_mode_seen = stream_mode
        for event in self._events:
            yield event


def _request() -> SimpleNamespace:
    return SimpleNamespace(
        context=None,
        interrupt_before=None,
        interrupt_after=None,
        durability="exit",
        stream_subgraphs=False,
    )


def _events() -> list[tuple[str, Any]]:
    return [
        ("messages", (_Msg("Hel"), {"langgraph_node": "agent"})),
        ("messages", (_Msg("lo"), {"langgraph_node": "agent"})),
        ("custom", {"type": "ui", "id": "ui-1"}),
        ("values", {"messages": [{"type": "ai", "content": "Hello", "id": "final"}]}),
    ]


async def test_accumulator_grows_values_and_forwards_custom() -> None:
    graph = _FakeGraph(_events())
    out = [
        (name, payload)
        async for name, payload in stream_incremental_values(
            graph, {"input": 1}, {}, _request(), agent_nodes=frozenset({"agent"})
        )
    ]

    # It subscribes to messages + values + custom on the underlying graph.
    assert graph.stream_mode_seen == ["messages", "values", "custom"]

    # Incremental "values" reveal the message growing token-by-token.
    ai_contents = [
        p["messages"][-1]["content"]
        for n, p in out
        if n == "values" and p.get("messages")
    ]
    assert "Hel" in ai_contents  # partial reveal before completion
    assert ai_contents[-1] == "Hello"  # final snapshot

    # The custom (UI) event is forwarded untouched.
    assert ("custom", {"type": "ui", "id": "ui-1"}) in out


async def test_non_agent_node_tokens_are_not_streamed() -> None:
    # Tokens from a node outside agent_nodes must not produce incremental values.
    graph = _FakeGraph(
        [
            ("messages", (_Msg("secret"), {"langgraph_node": "internal"})),
            ("values", {"messages": [{"type": "ai", "content": "done", "id": "f"}]}),
        ]
    )
    out = [
        (name, payload)
        async for name, payload in stream_incremental_values(
            graph, {"input": 1}, {}, _request(), agent_nodes=frozenset({"agent"})
        )
    ]
    # Only the single real "values" snapshot — no synthetic per-token events.
    assert [n for n, _ in out] == ["values"]


async def test_runner_routes_multi_mode_request_to_accumulator() -> None:
    # The real SDK request: values + messages-tuple + custom. With agent_nodes
    # configured this must engage the accumulator (membership, not equality).
    graph = _FakeGraph(_events())
    streamer = Streamer(graph, agent_nodes=frozenset({"agent"}))
    out = [
        (name, payload)
        async for name, payload in streamer.stream(
            {"input": 1}, {}, _request(), ["values", "messages-tuple", "custom"]
        )
    ]
    # Engaged the accumulator → underlying graph was asked for messages too.
    assert graph.stream_mode_seen == ["messages", "values", "custom"]
    ai_contents = [
        p["messages"][-1]["content"]
        for n, p in out
        if n == "values" and p.get("messages")
    ]
    assert "Hel" in ai_contents and ai_contents[-1] == "Hello"
    assert ("custom", {"type": "ui", "id": "ui-1"}) in out
