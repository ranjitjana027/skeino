"""Inbound normalizers that prepare HTTP payloads for the LangGraph runtime."""

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from langchain_core.messages.utils import convert_to_messages
from langgraph.types import Command

from skeino.schemas import CheckpointConfigModel, CommandModel, JsonValue


def normalize_input_payload(input_payload: JsonValue) -> Any:
    """Convert JSON request input into LangGraph-ready Python objects."""
    if isinstance(input_payload, dict):
        normalized_payload: dict[str, Any] = {}
        for key, value in input_payload.items():
            if key == "messages" and isinstance(value, list):
                normalized_payload[key] = list(convert_to_messages(value))
            else:
                normalized_payload[key] = normalize_input_payload(value)
        return normalized_payload

    if isinstance(input_payload, list):
        return [normalize_input_payload(item) for item in input_payload]

    return input_payload


def normalize_command_payload(command: CommandModel | None) -> Command | None:
    """Convert an HTTP command payload into a LangGraph command."""
    if command is None:
        return None
    return Command(
        update=normalize_input_payload(command.update),
        resume=normalize_input_payload(command.resume),
        goto=normalize_input_payload(command.goto),
    )


def coerce_stream_modes(stream_mode: str | Sequence[str]) -> list[str]:
    """Normalize stream mode inputs into a list."""
    if isinstance(stream_mode, str):
        return [stream_mode]
    return list(stream_mode)


def build_thread_config(
    thread_id: str,
    config: dict[str, JsonValue],
    checkpoint: CheckpointConfigModel | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Build the runnable config used for LangGraph calls."""
    merged_config: dict[str, Any] = {
        key: normalize_input_payload(value) for key, value in config.items()
    }
    configurable = dict(merged_config.get("configurable", {}))
    configurable["thread_id"] = thread_id

    if checkpoint is not None:
        if checkpoint.checkpoint_ns:
            configurable["checkpoint_ns"] = checkpoint.checkpoint_ns
        if checkpoint.checkpoint_id:
            configurable["checkpoint_id"] = checkpoint.checkpoint_id
        if checkpoint.checkpoint_map is not None:
            configurable["checkpoint_map"] = normalize_input_payload(
                checkpoint.checkpoint_map
            )

    merged_config["configurable"] = configurable

    if run_id is not None:
        merged_config["run_id"] = UUID(run_id)
        meta: dict[str, Any] = dict(merged_config.get("metadata", {}))
        meta.setdefault("run_id", run_id)
        meta.setdefault("thread_id", thread_id)
        merged_config["metadata"] = meta

    return merged_config
