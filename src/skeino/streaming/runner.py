"""Stream-mode dispatcher.

``Streamer.stream`` picks one of three execution paths:

* ``["events"]`` — proxy ``graph.astream_events`` (LangGraph v2 events).
* ``["values"]`` — token-level accumulation via
  :func:`stream_incremental_values`.
* anything else — straight ``graph.astream`` with the requested modes.

The runner is intentionally stateless across calls; per-stream state lives in
locals of the async generator.
"""

import logging
from typing import Any, AsyncIterator

from skeino.schemas import JsonValue, RunCreateRequest
from skeino.serialization import (
    normalize_input_payload,
    serialize_mapping,
    serialize_value,
)
from skeino.streaming.incremental import stream_incremental_values

logger = logging.getLogger(__name__)

# Stream modes the incremental accumulator can serve: it emits "values"
# token-by-token, makes "messages-tuple" redundant, and forwards "custom" (UI)
# events. A request limited to these (and including "values") is routed to the
# accumulator; any other mode (e.g. "updates") falls through to the generic path.
_ACCUMULATOR_MODES = frozenset({"values", "messages-tuple", "custom"})


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
            # No declared output schema → no filtering (everything passes).
            return None
        # Pydantic model schema: keys live in model_fields.
        model_fields = getattr(schema, "model_fields", None)
        if isinstance(model_fields, dict) and model_fields:
            return frozenset(model_fields.keys())
        # TypedDict / annotated-class schema (the common LangGraph case, e.g.
        # `StateGraph(State, output=OutputState)`): the declared keys are
        # introspectable via __annotations__, so we can still filter precisely
        # without leaking internal state — and without dropping `messages`.
        annotations = getattr(schema, "__annotations__", None)
        if isinstance(annotations, dict) and annotations:
            return frozenset(annotations.keys())
        # Genuinely opaque schema. Fail *closed*: an empty allow-set drops every
        # field rather than leaking internal pipeline state to clients.
        logger.warning(
            "Could not resolve output schema fields for %s; filtering out "
            "all streamed values to avoid leaking internal fields",
            type(self._graph).__name__,
        )
        return frozenset()

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
                yield "events", serialize_mapping(event)
            return

        # Token-by-token streaming. When the consumer opts in (agent_nodes set)
        # and the client wants "values", synthesize incremental "values" events
        # from the message stream so UIs can reveal text as the model speaks.
        # Real langgraph-sdk clients request ["values", "messages-tuple",
        # "custom"], so match on membership, not equality — an exact ["values"]
        # check never fires for them. The accumulator *covers* exactly those
        # modes (values incrementally; messages-tuple becomes redundant; custom
        # is forwarded), so only engage when every requested mode is one it
        # serves — a request for "updates" (or anything else) still needs the
        # generic path. "events" is exclusive (validated upstream), handled above.
        if (
            self._agent_nodes
            and "values" in stream_modes
            and set(stream_modes) <= _ACCUMULATOR_MODES
        ):
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
