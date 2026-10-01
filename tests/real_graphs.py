"""Real compiled LangGraph graphs for conformance tests.

``FakeGraph`` (``tests/conftest.py``) stays the tool for failure injection, but
it only emits the chunk shapes someone thought to give it — which is how a
``messages``-mode regression shipped unnoticed (#113). The graphs here are
genuine ``StateGraph`` compilations run through skeino's public
``create_app`` with in-memory persistence: no Docker, no LLM, milliseconds per
run, and every stream chunk has exactly the shape LangGraph really produces.

Each builder takes the checkpointer skeino resolves and returns the compiled
graph, matching ``create_app(graphs={...})``'s factory signature.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Annotated, Any, TypedDict

from fastapi.testclient import TestClient
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, AnyMessage
from langgraph.config import get_stream_writer
from langgraph.graph import START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.types import interrupt
from pydantic import BaseModel

from skeino import SkeinoSettings, create_app

ASSISTANT_ID = "agent"
FAKE_LLM_REPLY = "the quick brown fox"
INTERNAL_VALUE = "internal-pipeline-detail"
FAILURE_MESSAGE = "graph exploded"
GATE_TIMEOUT_SECONDS = 5.0

# Holds ``gated`` runs open until the test releases them; see ``gate()``.
_GATE = threading.Event()
_GATE.set()  # open by default: outside gate() ``gated`` behaves like ``echo``


class MessagesState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


class StateWithInternal(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    internal: str


class TypedOutput(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


class PydanticOutput(BaseModel):
    messages: list[AnyMessage]


class ParentState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    inner_result: str


def _last_text(state: dict[str, Any]) -> str:
    messages = state.get("messages") or []
    return str(messages[-1].content) if messages else ""


def build_echo(checkpointer: Any) -> Any:
    """One node that replies ``echo: <last message>``."""

    def reply(state: MessagesState) -> dict[str, Any]:
        return {"messages": [AIMessage(f"echo: {_last_text(state)}")]}

    graph = StateGraph(MessagesState)
    graph.add_node("reply", reply)
    graph.add_edge(START, "reply")
    return graph.compile(checkpointer=checkpointer)


def _build_with_internal(checkpointer: Any, output_schema: type) -> Any:
    def reply(state: StateWithInternal) -> dict[str, Any]:
        return {
            "messages": [AIMessage(f"echo: {_last_text(state)}")],
            "internal": INTERNAL_VALUE,
        }

    graph = StateGraph(StateWithInternal, output_schema=output_schema)
    graph.add_node("reply", reply)
    graph.add_edge(START, "reply")
    return graph.compile(checkpointer=checkpointer)


def build_typed_output(checkpointer: Any) -> Any:
    """State carries an ``internal`` key; the TypedDict output schema hides it."""
    return _build_with_internal(checkpointer, TypedOutput)


def build_pydantic_output(checkpointer: Any) -> Any:
    """Same as ``typed_output`` but with a Pydantic output schema."""
    return _build_with_internal(checkpointer, PydanticOutput)


def build_writer(checkpointer: Any) -> Any:
    """Emits ``custom`` events: a dict payload and a non-dict (string) payload."""

    def work(state: MessagesState) -> dict[str, Any]:
        write = get_stream_writer()
        write({"progress": 50})
        write("plain-text-progress")
        return {"messages": [AIMessage("done")]}

    graph = StateGraph(MessagesState)
    graph.add_node("work", work)
    graph.add_edge(START, "work")
    return graph.compile(checkpointer=checkpointer)


def build_interrupting(checkpointer: Any) -> Any:
    """Pauses on ``interrupt("approve?")``; resumes with the answer."""

    def ask(state: MessagesState) -> dict[str, Any]:
        answer = interrupt("approve?")
        return {"messages": [AIMessage(f"approved: {answer}")]}

    graph = StateGraph(MessagesState)
    graph.add_node("ask", ask)
    graph.add_edge(START, "ask")
    return graph.compile(checkpointer=checkpointer)


def build_gated(checkpointer: Any) -> Any:
    """Echo that blocks until the test opens ``gate()``, keeping its thread busy."""

    async def reply(state: MessagesState) -> dict[str, Any]:
        opened = await asyncio.to_thread(_GATE.wait, GATE_TIMEOUT_SECONDS)
        if not opened:
            raise TimeoutError("gated run never released; open gate() in the test")
        return {"messages": [AIMessage(f"echo: {_last_text(state)}")]}

    graph = StateGraph(MessagesState)
    graph.add_node("reply", reply)
    graph.add_edge(START, "reply")
    return graph.compile(checkpointer=checkpointer)


def build_failing(checkpointer: Any) -> Any:
    """One node that raises ``ValueError(FAILURE_MESSAGE)``."""

    def explode(state: MessagesState) -> dict[str, Any]:
        raise ValueError(FAILURE_MESSAGE)

    graph = StateGraph(MessagesState)
    graph.add_node("explode", explode)
    graph.add_edge(START, "explode")
    return graph.compile(checkpointer=checkpointer)


def build_with_subgraph(checkpointer: Any) -> Any:
    """Parent graph whose only node is a compiled subgraph."""

    def inner_step(state: ParentState) -> dict[str, Any]:
        return {"inner_result": f"inner saw: {_last_text(state)}"}

    inner = StateGraph(ParentState)
    inner.add_node("inner_step", inner_step)
    inner.add_edge(START, "inner_step")

    parent = StateGraph(ParentState)
    parent.add_node("inner", inner.compile())
    parent.add_edge(START, "inner")
    return parent.compile(checkpointer=checkpointer)


def build_fake_llm(checkpointer: Any) -> Any:
    """A chat-model node, so ``messages`` mode carries real token chunks."""
    model = GenericFakeChatModel(messages=itertools.cycle([AIMessage(FAKE_LLM_REPLY)]))

    def call_model(state: MessagesState) -> dict[str, Any]:
        return {"messages": [model.invoke(state["messages"])]}

    graph = StateGraph(MessagesState)
    graph.add_node("call_model", call_model)
    graph.add_edge(START, "call_model")
    return graph.compile(checkpointer=checkpointer)


GRAPHS: dict[str, Callable[[Any], Any]] = {
    "echo": build_echo,
    "typed_output": build_typed_output,
    "pydantic_output": build_pydantic_output,
    "writer": build_writer,
    "interrupting": build_interrupting,
    "with_subgraph": build_with_subgraph,
    "fake_llm": build_fake_llm,
    "gated": build_gated,
    "failing": build_failing,
}


@contextmanager
def real_client(graph_name: str) -> Iterator[TestClient]:
    """Yield a ``TestClient`` over skeino serving the named real graph."""
    app = create_app(
        graphs={ASSISTANT_ID: GRAPHS[graph_name]},
        settings=SkeinoSettings(default_assistant_id=ASSISTANT_ID),
    )
    with TestClient(app) as client:
        yield client


@contextmanager
def gate() -> Iterator[threading.Event]:
    """Close the ``gated`` graph's gate for the block; always reopen on exit.

    Inside the block ``gated`` runs stay busy; ``.set()`` releases them early.
    Reopening on exit means a failing test never leaves a run hanging.
    """
    _GATE.clear()
    try:
        yield _GATE
    finally:
        _GATE.set()


def user_input(text: str = "hi") -> dict[str, Any]:
    """Run ``input`` payload carrying one user message."""
    return {"messages": [{"role": "user", "content": text}]}


def parse_sse(body: str) -> list[tuple[str, Any]]:
    """Split an SSE body into ``(event, decoded data)`` pairs, in order."""
    events: list[tuple[str, Any]] = []
    for frame in body.split("\n\n"):
        name: str | None = None
        data_lines: list[str] = []
        for line in frame.splitlines():
            if line.startswith("event: "):
                name = line[len("event: ") :]
            elif line.startswith("data: "):
                data_lines.append(line[len("data: ") :])
        if name is None:
            continue
        raw = "\n".join(data_lines)
        events.append((name, json.loads(raw) if raw else None))
    return events
