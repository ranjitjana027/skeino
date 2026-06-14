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
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, LLMResult

from skeino import SkeinoSettings, create_app


def _utcnow() -> datetime:
    return datetime.now(UTC)


class _FakeCheckpointer:
    """Minimal checkpointer exposing the delete hook skeino calls."""

    def __init__(self, graph: FakeGraph) -> None:
        self._graph = graph

    async def adelete_thread(self, thread_id: str) -> None:
        self._graph.state_by_thread.pop(thread_id, None)
        self._graph.history_by_thread.pop(thread_id, None)
        self._graph.checkpoints_by_thread.pop(thread_id, None)


class FakeGraph:
    """Minimal stand-in for ``CompiledStateGraph`` used by tests."""

    def __init__(self) -> None:
        self.state_by_thread: dict[str, dict[str, Any]] = {}
        self.history_by_thread: dict[str, list[SimpleNamespace]] = {}
        # Ordered per-thread checkpoints: [{checkpoint_id, values}, ...] — lets
        # aget_state honour a checkpoint_id selector (time travel).
        self.checkpoints_by_thread: dict[str, list[dict[str, Any]]] = {}
        self.checkpointer = _FakeCheckpointer(self)
        self._checkpoint_seq = 0
        # --- Failure-injection hooks (default: fully cooperative) ---
        # When ``invoke_error`` is set, ``ainvoke`` raises it.
        self.invoke_error: BaseException | None = None
        # When ``stream_error`` is set, ``astream`` raises it after emitting
        # ``stream_error_after`` events. ``stream_fail_times`` limits failure to
        # the first N attempts (0 = every attempt), enabling retry scenarios.
        self.stream_error: BaseException | None = None
        self.stream_error_after: int = 0
        self.stream_fail_times: int = 0
        self.stream_attempts: int = 0
        # Simulated LLM token usage: when set, ainvoke/astream fire on_llm_end
        # on every handler in config["callbacks"], as a real chat model would.
        self.llm_usage: dict[str, int] | None = None
        self.llm_model_name: str = "fake-model"

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
        checkpoint_id = self._record_checkpoint(thread_id, state)
        self.history_by_thread.setdefault(thread_id, []).append(
            self._snapshot(thread_id, state, checkpoint_id)
        )
        return {
            "configurable": {"thread_id": thread_id, "checkpoint_id": checkpoint_id}
        }

    def _record_checkpoint(self, thread_id: str, values: dict[str, Any]) -> str:
        """Snapshot ``values`` under a fresh checkpoint id and return it."""
        self._checkpoint_seq += 1
        checkpoint_id = f"ckpt-{self._checkpoint_seq}"
        self.checkpoints_by_thread.setdefault(thread_id, []).append(
            {"checkpoint_id": checkpoint_id, "values": dict(values)}
        )
        return checkpoint_id

    async def aget_state(
        self, config: dict[str, Any], *, subgraphs: bool = False
    ) -> SimpleNamespace:
        del subgraphs
        configurable = config.get("configurable", {})
        thread_id = str(configurable["thread_id"])
        checkpoint_id = configurable.get("checkpoint_id")
        if checkpoint_id is not None:
            for ckpt in self.checkpoints_by_thread.get(thread_id, []):
                if ckpt["checkpoint_id"] == checkpoint_id:
                    return self._snapshot(thread_id, ckpt["values"], checkpoint_id)
            return self._snapshot(thread_id, {}, checkpoint_id)
        checkpoints = self.checkpoints_by_thread.get(thread_id, [])
        latest_id = checkpoints[-1]["checkpoint_id"] if checkpoints else None
        return self._snapshot(
            thread_id, self.state_by_thread.get(thread_id, {}), latest_id
        )

    def _fire_usage_callbacks(self, config: dict[str, Any]) -> None:
        """Simulate an LLM call's on_llm_end against the config's callbacks.

        Uses real langchain-core types so the actual handler logic runs —
        including UsageMetadataCallbackHandler's requirement that both
        usage_metadata and response_metadata["model_name"] be present.
        """
        if self.llm_usage is None:
            return
        message = AIMessage(
            content="",
            usage_metadata=self.llm_usage,  # type: ignore[arg-type]
            response_metadata={"model_name": self.llm_model_name},
        )
        result = LLMResult(generations=[[ChatGeneration(message=message)]])
        for handler in config.get("callbacks") or []:
            on_llm_end = getattr(handler, "on_llm_end", None)
            if callable(on_llm_end):
                on_llm_end(result)

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
        if self.invoke_error is not None:
            raise self.invoke_error
        self._fire_usage_callbacks(config)
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
        modes = (
            [stream_mode]
            if isinstance(stream_mode, str)
            else list(stream_mode or ["values"])
        )
        events: list[tuple[str, dict[str, Any]]] = []
        for mode in modes:
            if mode == "updates":
                events.append(("updates", {"messages": messages}))
            if mode == "values":
                events.append(("values", {"messages": final_messages}))

        self.stream_attempts += 1
        pending_error = self.stream_error
        if pending_error is not None and (
            self.stream_fail_times == 0
            or self.stream_attempts <= self.stream_fail_times
        ):
            emitted = 0
            for event in events:
                if emitted >= self.stream_error_after:
                    raise pending_error
                yield event
                emitted += 1
            raise pending_error

        self._fire_usage_callbacks(config)
        for event in events:
            yield event
        state["messages"] = final_messages
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

    def _snapshot(
        self,
        thread_id: str,
        values: dict[str, Any],
        checkpoint_id: str | None = None,
    ) -> SimpleNamespace:
        configurable: dict[str, Any] = {"thread_id": thread_id}
        if checkpoint_id is not None:
            configurable["checkpoint_id"] = checkpoint_id
        return SimpleNamespace(
            values=dict(values),
            next=(),
            tasks=(),
            config={"configurable": configurable},
            metadata={},
            created_at=_utcnow(),
            parent_config=None,
            interrupts=(),
        )


def build_test_app(
    *,
    assistant_id: str = "test_agent",
) -> tuple[FastAPI, FakeGraph]:
    """Build a skeino FastAPI app backed by a fresh FakeGraph + in-memory store."""
    graph = FakeGraph()
    settings = SkeinoSettings(
        default_assistant_id=assistant_id,
        assistant_name="Test Agent",
        assistant_description="skeino test agent",
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
    return build_test_app()


@pytest.fixture
def skeino_client(skeino_app_and_graph: tuple[FastAPI, FakeGraph]) -> TestClient:
    """Return a ``TestClient`` against a skeino app with lifespan applied."""
    app, _ = skeino_app_and_graph
    with TestClient(app) as client:
        yield client
