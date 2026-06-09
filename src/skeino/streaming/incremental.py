"""Token-level value streaming.

The standard LangGraph ``values`` stream mode emits one snapshot per node. For
UIs that want token-by-token updates as the model speaks, we instead subscribe
to both ``messages`` (per-token chunks) and ``values`` (state snapshots),
accumulate the chunks for designated *agent nodes*, and re-emit ``values``
events with the live message appended to the previous-turn message list.

The set of nodes considered "speaking" is graph-specific — pass it in via
``agent_nodes``. An optional ``status_field`` lets a graph surface progress
strings from a list-typed state field as individual ``status`` events.
"""

from typing import Any, AsyncIterator
from uuid import uuid4

from skeino.schemas import JsonValue, RunCreateRequest
from skeino.serialization import (
    normalize_input_payload,
    serialize_mapping,
    serialize_value,
)


async def stream_incremental_values(
    graph: Any,
    runnable_input: Any,
    config: dict[str, Any],
    request: RunCreateRequest,
    *,
    agent_nodes: frozenset[str] = frozenset(),
    status_field: str | None = None,
) -> AsyncIterator[tuple[str, dict[str, JsonValue]]]:
    """Yield ``values`` (and optional ``status``) events as the graph streams."""
    last_values: dict[str, JsonValue] = {}
    streaming_node: str | None = None
    streaming_id: str | None = None
    accumulated_content = ""
    accumulated_tool_calls: list[dict[str, JsonValue]] = []
    previous_messages: list[dict[str, JsonValue]] = []
    emitted_status_count: int = 0

    async for chunk in graph.astream(
        runnable_input,
        config,
        context=normalize_input_payload(request.context),
        stream_mode=["messages", "values", "custom"],
        interrupt_before=request.interrupt_before,
        interrupt_after=request.interrupt_after,
        durability=request.durability,
        subgraphs=request.stream_subgraphs,
    ):
        if isinstance(chunk, tuple) and len(chunk) == 3:
            event_name, payload = str(chunk[1]), chunk[2]
        elif isinstance(chunk, tuple) and len(chunk) == 2:
            event_name, payload = str(chunk[0]), chunk[1]
        else:
            continue

        if event_name == "custom":
            # Pass through graph-emitted custom (UI) events untouched so
            # generative-UI consumers keep working alongside token streaming.
            serialized = serialize_value(payload)
            if isinstance(serialized, dict):
                yield "custom", serialized
            continue

        if event_name == "values":
            last_values = serialize_mapping(payload)
            raw_messages = last_values.get("messages", [])
            previous_messages = (
                list(raw_messages) if isinstance(raw_messages, list) else []
            )

            if status_field is not None:
                status_items = last_values.get(status_field) or []
                if isinstance(status_items, list):
                    new_items = status_items[emitted_status_count:]
                    for item in new_items:
                        yield "status", {"message": str(item)}
                    emitted_status_count = len(status_items)

            yield "values", last_values
            streaming_node = None
            streaming_id = None
            accumulated_content = ""
            accumulated_tool_calls = []
            continue

        if event_name == "messages":
            msg_chunk, meta = payload
            node = str(meta.get("langgraph_node", "")) if isinstance(meta, dict) else ""
            if node not in agent_nodes:
                continue
            if node and node != streaming_node:
                streaming_node = node
                streaming_id = streaming_id or f"ai_{uuid4().hex[:8]}"
                accumulated_content = ""
                accumulated_tool_calls = []
            content = (
                getattr(msg_chunk, "content", None) or ""
                if hasattr(msg_chunk, "content")
                else ""
            )
            if isinstance(content, str):
                accumulated_content += content
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and "text" in block:
                        accumulated_content += str(block["text"])
                    elif isinstance(block, str):
                        accumulated_content += block
            tool_calls = getattr(msg_chunk, "tool_calls", None) or []
            if tool_calls:
                accumulated_tool_calls = [
                    {
                        "id": str(tc.get("id", "") or uuid4().hex[:12]),
                        "name": str(tc.get("name", "")),
                        "args": serialize_value(tc.get("args", {})),
                    }
                    for tc in tool_calls
                    if isinstance(tc, dict)
                ]
            streaming_id = streaming_id or f"ai_{uuid4().hex[:8]}"
            if not accumulated_content and not accumulated_tool_calls:
                continue
            in_progress: dict[str, JsonValue] = {
                "id": streaming_id,
                "type": "ai",
                "content": accumulated_content,
            }
            if accumulated_tool_calls:
                in_progress["tool_calls"] = accumulated_tool_calls
            combined = previous_messages + [in_progress]
            yield "values", {"messages": combined}
