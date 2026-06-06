"""Schemas for run creation, command payloads, and run responses."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from skeino.schemas.common import (
    DEFAULT_STREAM_MODES,
    CheckpointConfigModel,
    JsonValue,
    MultitaskStrategy,
    RunIfNotExists,
    RunStatus,
    StreamMode,
)


class CommandModel(BaseModel):
    """Serializable LangGraph command payload."""

    update: dict[str, JsonValue] | list[JsonValue] | None = None
    resume: JsonValue = None
    goto: JsonValue = None


class RunCreateRequest(BaseModel):
    """Payload for creating a run on an existing thread."""

    assistant_id: str
    checkpoint: CheckpointConfigModel | None = None
    input: JsonValue = None
    command: CommandModel | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    config: dict[str, JsonValue] = Field(default_factory=dict)
    context: dict[str, JsonValue] = Field(default_factory=dict)
    webhook: str | None = None
    interrupt_before: Literal["*"] | list[str] | None = None
    interrupt_after: Literal["*"] | list[str] | None = None
    stream_mode: StreamMode | list[StreamMode] = Field(
        default_factory=lambda: list(DEFAULT_STREAM_MODES)
    )
    stream_subgraphs: bool = False
    stream_resumable: bool = False
    on_disconnect: Literal["cancel", "continue"] = "continue"
    feedback_keys: list[str] = Field(default_factory=list)
    multitask_strategy: MultitaskStrategy = "enqueue"
    if_not_exists: RunIfNotExists = "reject"
    after_seconds: float | None = None
    checkpoint_during: bool = False
    durability: Literal["sync", "async", "exit"] = "exit"


class RunModel(BaseModel):
    """LangGraph-compatible run metadata."""

    run_id: UUID
    thread_id: UUID
    assistant_id: str
    created_at: str
    updated_at: str
    status: RunStatus
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    kwargs: dict[str, JsonValue] = Field(default_factory=dict)
    multitask_strategy: MultitaskStrategy
