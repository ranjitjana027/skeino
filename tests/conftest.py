"""Fixtures shared across skeino unit + integration tests.

``FakeGraph`` mirrors the public surface that ``skeino`` calls on a compiled
LangGraph graph — enough for the SDK shape, none of the heavy LLM machinery.
``build_test_app`` wires a TestClient-ready FastAPI app over the fake graph.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from skeino import SkeinoSettings, create_app


def _utcnow() -> datetime:
    return datetime.now(UTC)


class FakeGraph:
    """Minimal stand-in for ``CompiledStateGraph`` used by tests."""

    def __init__(self) -> None:
        self.state_by_thread: dict[str, dict[str, Any]] = {}
        self.history_by_thread: dict[str, list[SimpleNamespace]] = {}

    async def aupdate_state(
        self,
        config: dict[str, Any],
        values: dict[str, Any] | Any,
        as_node: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        del as_node, task_id
        thread_id = str(config["configurable"]["thread_id"])
        state = self.state_by_thread.setdefault(thread_id, {})
        if isinstance(values, dict):
            state.update(values)
        else:
            state["data"] = values
        self.history_by_thread.setdefault(thread_id, []).append(
            self._snapshot(thread_id, state)
        )
        return config

    async def aget_state(
        self, config: dict[str, Any], *, subgraphs: bool = False
    ) -> SimpleNamespace:
        del subgraphs
        thread_id = str(config["configurable"]["thread_id"])
        values = self.state_by_thread.get(thread_id, {})
        return self._snapshot(thread_id, values)

    async def ainvoke(
        self,
        input_value: dict[str, Any] | Any,
        config: dict[str, Any],
        *,
        context: dict[str, Any] | None = None,
        stream_mode: str = "values",
        interrupt_before: str | list[str] | None = None,
        interrupt_after: str | list[str] | None = None,
        durability: str | None = None,
    ) -> dict[str, Any]:
        del context, stream_mode, interrupt_before, interrupt_after, durability
        thread_id = str(config["configurable"]["thread_id"])
        state = self.state_by_thread.setdefault(thread_id, {})
        if isinstance(input_value, dict) and "messages" in input_value:
            messages = list(input_value["messages"])
            messages.append({"type": "ai", "content": "completed"})
            state["messages"] = messages
        elif isinstance(input_value, dict):
            state.update(input_value)
        else:
            state["data"] = input_value
        self.history_by_thread.setdefault(thread_id, []).append(
            self._snapshot(thread_id, state)
        )
        return state

    async def astream(
        self,
        input_value: dict[str, Any] | Any,
        config: dict[str, Any],
        *,
        context: dict[str, Any] | None = None,
        stream_mode: str | list[str] | None = None,
        interrupt_before: str | list[str] | None = None,
        interrupt_after: str | list[str] | None = None,
        durability: str | None = None,
        subgraphs: bool = False,
        debug: bool | None = None,
    ):
        del context, interrupt_before, interrupt_after, durability, subgraphs, debug
        thread_id = str(config["configurable"]["thread_id"])
        state = self.state_by_thread.setdefault(thread_id, {})
        messages: list[Any] = []
        if isinstance(input_value, dict) and "messages" in input_value:
            messages = list(input_value["messages"])
        final_messages = messages + [{"type": "ai", "content": "streamed"}]
        state["messages"] = final_messages
        modes = (
            [stream_mode]
            if isinstance(stream_mode, str)
            else list(stream_mode or ["values"])
        )
        for mode in modes:
            if mode == "updates":
                yield ("updates", {"messages": messages})
            if mode == "values":
                yield ("values", {"messages": final_messages})
        self.history_by_thread.setdefault(thread_id, []).append(
            self._snapshot(thread_id, state)
        )

    async def astream_events(
        self,
        input_value: dict[str, Any] | Any,
        config: dict[str, Any],
        *,
        version: str = "v2",
        interrupt_before: str | list[str] | None = None,
        interrupt_after: str | list[str] | None = None,
        durability: str | None = None,
    ):
        del input_value, config, version
        del interrupt_before, interrupt_after, durability
        yield {"event": "on_chain_start", "data": {"input": "ok"}}

    def get_input_jsonschema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"messages": {"type": "array"}}}

    def get_output_jsonschema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {"messages": {"type": "array"}}}

    def get_config_jsonschema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {}}

    def get_context_jsonschema(self) -> dict[str, Any]:
        return {"type": "object", "properties": {}}

    def get_graph(self, *, xray: bool | int = False) -> Any:
        self.last_xray = xray
        g = SimpleNamespace()
        g.to_json = lambda: {"nodes": [{"id": "router"}], "edges": []}
        return g

    def get_subgraphs(self, *, recurse: bool = False) -> Any:
        self.last_recurse = recurse
        return iter(())

    async def aget_state_history(
        self,
        config: dict[str, Any],
        *,
        filter: dict[str, Any] | None = None,
        before: dict[str, Any] | None = None,
        limit: int | None = None,
    ):
        del filter, before
        thread_id = str(config["configurable"]["thread_id"])
        history = list(reversed(self.history_by_thread.get(thread_id, [])))
        if limit is not None:
            history = history[:limit]
        for snapshot in history:
            yield snapshot

    def _snapshot(self, thread_id: str, values: dict[str, Any]) -> SimpleNamespace:
        return SimpleNamespace(
            values=dict(values),
            next=(),
            tasks=(),
            config={"configurable": {"thread_id": thread_id}},
            metadata={},
            created_at=_utcnow(),
            parent_config=None,
            interrupts=(),
        )


def build_test_app(
    *,
    assistant_id: str = "test_agent",
    agent_nodes: frozenset[str] | None = None,
    status_field: str | None = None,
) -> tuple[FastAPI, FakeGraph]:
    """Build a skeino FastAPI app backed by a fresh FakeGraph + in-memory store."""
    graph = FakeGraph()
    settings = SkeinoSettings(
        default_assistant_id=assistant_id,
        assistant_name="Test Agent",
        assistant_description="skeino test agent",
        agent_nodes=agent_nodes or frozenset(),
        status_field=status_field,
        server_version="0.0.1-test",
        welcome_message="hello",
    )
    app = create_app(
        graphs={assistant_id: lambda _ckpt: graph},
        settings=settings,
    )
    return app, graph


@pytest.fixture
def fake_graph() -> FakeGraph:
    """Return a fresh ``FakeGraph`` per test."""
    return FakeGraph()


@pytest.fixture
def skeino_app_and_graph() -> tuple[FastAPI, FakeGraph]:
    """Return a FastAPI app + FakeGraph wired through ``create_app``."""
    return build_test_app(
        agent_nodes=frozenset({"simple"}), status_field="pipeline_status"
    )


@pytest.fixture
def skeino_client(skeino_app_and_graph: tuple[FastAPI, FakeGraph]) -> TestClient:
    """Return a ``TestClient`` against a skeino app with lifespan applied."""
    app, _ = skeino_app_and_graph
    with TestClient(app) as client:
        yield client
