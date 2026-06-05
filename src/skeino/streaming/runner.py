"""Stream-mode dispatcher.

``Streamer.stream`` picks one of three execution paths:

* ``["events"]`` — proxy ``graph.astream_events`` (LangGraph v2 events).
* ``["values"]`` — token-level accumulation via
  :func:`stream_incremental_values`.
* anything else — straight ``graph.astream`` with the requested modes.

The runner is intentionally stateless across calls; per-stream state lives in
locals of the async generator.
"""

from typing import Any, AsyncIterator

from skeino.schemas import JsonValue, RunCreateRequest
from skeino.serialization import normalize_input_payload, serialize_value
from skeino.streaming.incremental import stream_incremental_values


class Streamer:
    """Dispatch graph streams across the supported LangGraph stream modes."""

    def __init__(
        self,
        graph: Any,
        *,
        agent_nodes: frozenset[str] = frozenset(),
        status_field: str | None = None,
    ) -> None:
        """Capture the graph and the policy for incremental message streaming."""
        self._graph = graph
        self._agent_nodes = agent_nodes
        self._status_field = status_field
        self._output_keys: frozenset[str] | None = self._resolve_output_keys()

    def _resolve_output_keys(self) -> frozenset[str] | None:
        schema = getattr(self._graph, "output_schema", None)
        if schema is None:
            return None
        try:
            return frozenset(schema.model_fields.keys())
        except Exception:
            return None

    def _filter_values(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        if self._output_keys is None:
            return payload
        return {k: v for k, v in payload.items() if k in self._output_keys}

    async def stream(
        self,
        runnable_input: Any,
        config: dict[str, Any],
        request: RunCreateRequest,
        stream_modes: list[str],
    ) -> AsyncIterator[tuple[str, dict[str, JsonValue]]]:
        """Yield ``(event_name, payload)`` tuples for one streaming run."""
        if stream_modes == ["events"]:
            async for event in self._graph.astream_events(
                runnable_input,
                config,
                version="v2",
                interrupt_before=request.interrupt_before,
                interrupt_after=request.interrupt_after,
                durability=request.durability,
            ):
                yield "events", serialize_value(event)
            return

        if stream_modes == ["values"]:
            async for event_name, payload in stream_incremental_values(
                self._graph,
                runnable_input,
                config,
                request,
                agent_nodes=self._agent_nodes,
                status_field=self._status_field,
            ):
                if event_name == "values" and isinstance(payload, dict):
                    payload = self._filter_values(payload)
                yield event_name, payload
            return

        async for chunk in self._graph.astream(
            runnable_input,
            config,
            context=normalize_input_payload(request.context),
            stream_mode=stream_modes if len(stream_modes) > 1 else stream_modes[0],
            interrupt_before=request.interrupt_before,
            interrupt_after=request.interrupt_after,
            durability=request.durability,
            subgraphs=request.stream_subgraphs,
        ):
            if isinstance(chunk, tuple) and len(chunk) == 3:
                event_name = str(chunk[1])
                event_payload = serialize_value(chunk[2])
            elif isinstance(chunk, tuple) and len(chunk) == 2:
                event_name = str(chunk[0])
                event_payload = serialize_value(chunk[1])
            else:
                event_name = stream_modes[0]
                event_payload = serialize_value(chunk)
            if isinstance(event_payload, dict):
                if event_name == "values":
                    event_payload = self._filter_values(event_payload)
                yield event_name, event_payload
