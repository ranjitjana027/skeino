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

    update: dict[str, JsonValue] | list[JsonValue] | None = Field(
        default=None, description="State update to apply before resuming the graph."
    )
    resume: JsonValue = Field(
        default=None,
        description="Value to resume an interrupted graph with (the ``interrupt()`` return).",
    )
    goto: JsonValue = Field(
        default=None, description="Node(s) to jump to when resuming the graph."
    )


class RunCreateRequest(BaseModel):
    """Payload for creating a run on an existing thread."""

    assistant_id: str = Field(description="Which assistant/graph to run (required).")
    checkpoint: CheckpointConfigModel | None = Field(
        default=None, description="Resume from a specific checkpoint."
    )
    input: JsonValue = Field(
        default=None,
        description="New state to merge in; ``messages`` is converted to LangChain messages.",
    )
    command: CommandModel | None = Field(
        default=None,
        description="Resume an interrupted graph (update/resume/goto). Provide either input or command, not both.",
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Metadata passed through to the run."
    )
    config: dict[str, JsonValue] = Field(
        default_factory=dict,
        description="Config passed through to the graph invocation.",
    )
    context: dict[str, JsonValue] = Field(
        default_factory=dict,
        description="Context passed through to the graph invocation.",
    )
    webhook: str | None = Field(
        default=None,
        description="LangGraph Platform option accepted by the schema but rejected at runtime as out of scope for v1.",
    )
    interrupt_before: Literal["*"] | list[str] | None = Field(
        default=None,
        description='Nodes to interrupt before; ``"*"`` for all.',
    )
    interrupt_after: Literal["*"] | list[str] | None = Field(
        default=None,
        description='Nodes to interrupt after; ``"*"`` for all.',
    )
    stream_mode: StreamMode | list[StreamMode] = Field(
        default_factory=lambda: list(DEFAULT_STREAM_MODES),
        description='Streaming mode(s) for the run; defaults to ``["values"]``.',
    )
    stream_subgraphs: bool = Field(
        default=False, description="Whether to include subgraph events in the stream."
    )
    stream_resumable: bool = Field(
        default=False, description="Whether the stream can be resumed after disconnect."
    )
    on_disconnect: Literal["cancel", "continue"] = Field(
        default="continue",
        description="What to do with the run if the stream client disconnects.",
    )
    feedback_keys: list[str] = Field(
        default_factory=list, description="Feedback keys to associate with the run."
    )
    multitask_strategy: MultitaskStrategy = Field(
        default="enqueue",
        description="Behaviour when the thread is busy (enqueue/reject/rollback/interrupt).",
    )
    if_not_exists: RunIfNotExists = Field(
        default="reject",
        description='Use ``"create"`` to auto-create a missing thread, ``"reject"`` to 404.',
    )
    after_seconds: float | None = Field(
        default=None,
        description="LangGraph Platform scheduled-run option accepted by the schema but rejected at runtime as out of scope for v1.",
    )
    checkpoint_during: bool = Field(
        default=False, description="Whether to persist checkpoints during the run."
    )
    durability: Literal["sync", "async", "exit"] = Field(
        default="exit",
        description="When checkpoints are written relative to graph steps.",
    )


class RunModel(BaseModel):
    """LangGraph-compatible run metadata."""

    run_id: UUID = Field(description="Unique identifier of the run.")
    thread_id: UUID = Field(description="Thread the run belongs to.")
    assistant_id: str = Field(description="Assistant/graph the run executed.")
    created_at: str = Field(description="ISO-8601 timestamp when the run was created.")
    updated_at: str = Field(
        description="ISO-8601 timestamp when the run was last updated."
    )
    status: RunStatus = Field(
        description="Run lifecycle status (pending/running/success/error/timeout/interrupted)."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Metadata stored with the run."
    )
    kwargs: dict[str, JsonValue] = Field(
        default_factory=dict, description="Stored run creation arguments."
    )
    multitask_strategy: MultitaskStrategy = Field(
        description="Multitask strategy the run was created with."
    )
