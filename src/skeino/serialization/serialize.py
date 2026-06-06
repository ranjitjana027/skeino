"""Outbound serializers that turn runtime objects into JSON-safe payloads."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from langchain_core.messages import BaseMessage

from skeino.schemas import (
    CheckpointConfigModel,
    InterruptModel,
    JsonValue,
    ThreadStateModel,
    ThreadTaskModel,
)


def _flatten_message_content(content: Any) -> str:
    """Flatten message content to a string for frontend display.

    LangChain messages may have content as a string or as a list of content
    blocks (e.g. [{"type": "text", "text": "..."}]). Return a single string.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict):
                if "text" in block:
                    parts.append(str(block["text"]))
            elif isinstance(block, str):
                parts.append(block)
        return "".join(parts)
    return str(content) if content is not None else ""


def _serialize_tool_call(tc: dict[str, Any]) -> dict[str, JsonValue]:
    """Serialize a single tool call for frontend."""
    return {
        "id": str(tc.get("id", "") or uuid4().hex[:12]),
        "name": str(tc.get("name", "")),
        "args": serialize_value(tc.get("args", {})),
    }


def serialize_message(
    message: BaseMessage,
    *,
    message_id: str | None = None,
    content_override: str | None = None,
    tool_calls_override: list[dict[str, JsonValue]] | None = None,
) -> dict[str, JsonValue]:
    """Convert a LangChain message to a frontend-compatible dictionary.

    Produces shape for AI: {"id","type":"ai","content","tool_calls"?}
    For tool: {"id","type":"tool","tool_call_id","name","content"}
    """
    dumped = message.model_dump()
    msg_type = str(dumped.get("type", "ai"))

    if msg_type == "tool":
        tid = message_id or dumped.get("id")
        if not tid and getattr(message, "id", None):
            tid = str(message.id)
        if not tid:
            tid = uuid4().hex[:12]
        return {
            "id": tid,
            "type": "tool",
            "tool_call_id": str(dumped.get("tool_call_id", "")),
            "name": str(dumped.get("name", "")),
            "content": _flatten_message_content(dumped.get("content")),
        }

    msg_id = message_id or dumped.get("id")
    if not msg_id and hasattr(message, "id") and message.id:
        msg_id = str(message.id)
    if not msg_id:
        msg_id = uuid4().hex[:12]
    content = (
        content_override
        if content_override is not None
        else _flatten_message_content(dumped.get("content"))
    )
    out: dict[str, JsonValue] = {
        "id": msg_id,
        "type": msg_type,
        "content": content,
    }
    raw_tool_calls = (
        tool_calls_override
        if tool_calls_override is not None
        else dumped.get("tool_calls") or []
    )
    if raw_tool_calls:
        out["tool_calls"] = [
            _serialize_tool_call(tc) if isinstance(tc, dict) else serialize_value(tc)
            for tc in raw_tool_calls
        ]
    # Graph nodes can attach app-specific metadata to a message via
    # additional_kwargs (e.g. pipeline_status snapshots for the activity log).
    # Pass it through when non-empty so the streaming frontend can read it;
    # leave it off otherwise to keep the wire format lean.
    raw_additional_kwargs = dumped.get("additional_kwargs") or {}
    if isinstance(raw_additional_kwargs, dict) and raw_additional_kwargs:
        out["additional_kwargs"] = serialize_value(raw_additional_kwargs)
    return out


def serialize_value(value: Any) -> JsonValue:
    """Recursively convert runtime objects into JSON-safe values."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, BaseMessage):
        return serialize_message(value)

    if isinstance(value, dict):
        return {str(key): serialize_value(item) for key, item in value.items()}

    if isinstance(value, (list, tuple)):
        return [serialize_value(item) for item in value]

    if hasattr(value, "_asdict"):
        return serialize_value(value._asdict())

    if hasattr(value, "__dict__"):
        serializable_dict = {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_") and not callable(item)
        }
        return serialize_value(serializable_dict)

    return str(value)


def serialize_mapping(value: Any) -> dict[str, JsonValue]:
    """Serialize a value expected to be a mapping into a JSON-safe dict.

    Non-mapping inputs collapse to an empty dict. Use at call sites whose API
    contract guarantees a dict (metadata, config, JSON schemas) so the result
    matches the ``dict[str, JsonValue]`` field type.
    """
    serialized = serialize_value(value)
    return serialized if isinstance(serialized, dict) else {}


def serialize_optional_mapping(value: Any) -> dict[str, JsonValue] | None:
    """Serialize an optional mapping, preserving ``None`` for absent values."""
    if value is None:
        return None
    serialized = serialize_value(value)
    return serialized if isinstance(serialized, dict) else None


def serialize_collection(value: Any) -> dict[str, JsonValue] | list[JsonValue]:
    """Serialize a value expected to be a dict or list into its JSON-safe form."""
    serialized = serialize_value(value)
    if isinstance(serialized, (dict, list)):
        return serialized
    return {}


def _serialize_snapshot_message(message: BaseMessage) -> JsonValue:
    """Serialize a LangChain message using its full schema for Studio compatibility."""
    return serialize_value(message.model_dump())


def serialize_snapshot_value(value: Any) -> JsonValue:
    """Serialize state snapshot values preserving full LangChain message schemas.

    LangGraph Studio requires the complete message dict (additional_kwargs,
    response_metadata, name, tool_calls, etc.) to render state panels correctly.
    This is distinct from ``serialize_value`` which produces a stripped-down format
    for the streaming frontend.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, BaseMessage):
        return _serialize_snapshot_message(value)

    if isinstance(value, dict):
        return {str(key): serialize_snapshot_value(item) for key, item in value.items()}

    if isinstance(value, (list, tuple)):
        return [serialize_snapshot_value(item) for item in value]

    if hasattr(value, "_asdict"):
        return serialize_snapshot_value(value._asdict())

    if hasattr(value, "__dict__"):
        serializable_dict = {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_") and not callable(item)
        }
        return serialize_snapshot_value(serializable_dict)

    return str(value)


def serialize_interrupt(interrupt: Any) -> InterruptModel:
    """Convert a runtime interrupt into its API representation."""
    raw_value = getattr(interrupt, "value", interrupt)
    interrupt_id = getattr(interrupt, "id", None)
    serialized_value = serialize_value(raw_value)
    if not isinstance(serialized_value, dict):
        serialized_value = {"value": serialized_value}
    return InterruptModel(
        id=str(interrupt_id) if interrupt_id is not None else None,
        value=serialized_value,
    )


def serialize_task(task: Any) -> ThreadTaskModel:
    """Convert a runtime task into its API representation."""
    task_interrupts = getattr(task, "interrupts", ()) or ()
    task_checkpoint = getattr(task, "checkpoint", None)
    task_state = getattr(task, "state", None)
    serialized_checkpoint = (
        CheckpointConfigModel(
            thread_id=getattr(task_checkpoint, "thread_id", None),
            checkpoint_ns=getattr(task_checkpoint, "checkpoint_ns", None),
            checkpoint_id=getattr(task_checkpoint, "checkpoint_id", None),
            checkpoint_map=serialize_optional_mapping(
                getattr(task_checkpoint, "checkpoint_map", None)
            ),
        )
        if task_checkpoint is not None
        else None
    )
    return ThreadTaskModel(
        id=str(getattr(task, "id")),
        name=str(getattr(task, "name")),
        error=getattr(task, "error", None),
        interrupts=[serialize_interrupt(interrupt) for interrupt in task_interrupts],
        checkpoint=serialized_checkpoint,
        state=serialize_optional_mapping(task_state),
    )


def serialize_state_snapshot(snapshot: Any) -> ThreadStateModel:
    """Convert a LangGraph state snapshot into the API response model."""
    checkpoint_config = snapshot.config.get("configurable", {})
    parent_config = getattr(snapshot, "parent_config", None)
    return ThreadStateModel(
        values=serialize_collection(snapshot.values),
        next=[str(item) for item in snapshot.next],
        tasks=[serialize_task(task) for task in snapshot.tasks],
        checkpoint=CheckpointConfigModel(
            thread_id=checkpoint_config.get("thread_id"),
            checkpoint_ns=checkpoint_config.get("checkpoint_ns"),
            checkpoint_id=checkpoint_config.get("checkpoint_id"),
            checkpoint_map=serialize_optional_mapping(
                checkpoint_config.get("checkpoint_map")
            ),
        ),
        metadata=serialize_mapping(snapshot.metadata or {}),
        created_at=(
            snapshot.created_at.isoformat()
            if isinstance(snapshot.created_at, datetime)
            else snapshot.created_at
        ),
        parent_checkpoint=serialize_optional_mapping(parent_config),
        interrupts=[
            serialize_interrupt(interrupt)
            for interrupt in (getattr(snapshot, "interrupts", ()) or ())
        ],
    )
