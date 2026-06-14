"""Stream-mode dispatcher.

``Streamer.stream`` picks one of two execution paths:

* ``["events"]`` — proxy ``graph.astream_events`` (LangGraph v2 events).
* anything else — straight ``graph.astream`` with the requested modes, faithful
  to a real LangGraph server: ``values`` = full state per super-step,
  ``updates`` = per-node deltas, ``messages``/``messages-tuple`` = message
  chunks, ``custom`` = graph-emitted writer events.

State-bearing events (``values`` and ``updates``) are passed through a
fail-closed output-key filter so internal pipeline fields never leak to clients.

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

logger = logging.getLogger(__name__)


class Streamer:
    """Dispatch graph streams across the supported LangGraph stream modes."""

    def __init__(self, graph: Any) -> None:
        """Capture the graph and resolve its declared output keys for filtering."""
        self._graph = graph
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
        """Drop non-output keys from a ``values`` (full-state) snapshot."""
        if self._output_keys is None:
            return payload
        return {k: v for k, v in payload.items() if k in self._output_keys}

    def _filter_updates(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Drop non-output keys from each node's delta in an ``updates`` event.

        An ``updates`` payload is ``{node_name: {state_key: value}}``. Node
        deltas carry internal pipeline fields (routing metadata, raw reports,
        etc.), so apply the same fail-closed allow-set to each node's update.
        """
        if self._output_keys is None:
            return payload
        filtered: dict[str, JsonValue] = {}
        for node, update in payload.items():
            if isinstance(update, dict):
                filtered[node] = {
                    k: v for k, v in update.items() if k in self._output_keys
                }
            else:
                # Non-dict node update (rare); pass through unchanged — it
                # carries no named state keys to leak.
                filtered[node] = update
        return filtered

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
                elif event_name == "updates":
                    event_payload = self._filter_updates(event_payload)
                yield event_name, event_payload
