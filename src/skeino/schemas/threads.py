"""Schemas for thread creation, search, state, and history endpoints."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from skeino.schemas.common import (
    CheckpointConfigModel,
    JsonValue,
    ThreadIfExists,
    ThreadStatus,
)
from skeino.schemas.runs import CommandModel


class ThreadTtlConfig(BaseModel):
    """Time-to-live settings for a thread."""

    strategy: Literal["delete", "keep_latest"] = Field(
        default="delete",
        description="What to do when the thread expires: delete it or keep only its latest checkpoint.",
    )
    ttl: float | None = Field(
        default=None, description="Time-to-live in minutes; ``None`` means no expiry."
    )


class ThreadTtlInfo(BaseModel):
    """TTL information returned for a thread."""

    strategy: Literal["delete", "keep_latest"] = Field(
        description="Expiry strategy applied to the thread."
    )
    ttl_minutes: float = Field(description="Time-to-live in minutes.")
    expires_at: str = Field(
        description="ISO-8601 timestamp at which the thread expires."
    )


class ThreadSuperstepUpdate(BaseModel):
    """Initial state update applied during thread creation."""

    values: dict[str, JsonValue] | list[JsonValue] | None = Field(
        default=None, description="State values to write as part of this update."
    )
    command: CommandModel | None = Field(
        default=None, description="Command to apply instead of (or alongside) values."
    )
    as_node: str = Field(
        description="Node the update is attributed to in the checkpoint graph."
    )


class ThreadSuperstep(BaseModel):
    """A superstep container for thread bootstrap updates."""

    updates: list[ThreadSuperstepUpdate] = Field(
        description="Ordered state updates applied as one superstep."
    )


class ThreadCreateRequest(BaseModel):
    """Payload for creating a thread."""

    thread_id: UUID | None = Field(
        default=None, description="Optional explicit thread id; generated if omitted."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Arbitrary metadata stored with the thread."
    )
    if_exists: ThreadIfExists = Field(
        default="raise",
        description="Behaviour when the id already exists: ``raise`` (409) or ``do_nothing``.",
    )
    ttl: ThreadTtlConfig | None = Field(
        default=None, description="Optional time-to-live configuration for the thread."
    )
    supersteps: list[ThreadSuperstep] = Field(
        default_factory=list,
        description="Optional initial state updates to seed the thread on creation.",
    )


class ThreadPatchRequest(BaseModel):
    """Mutable fields updatable on an existing thread.

    ``metadata`` is optional so an empty body is a no-op; send
    ``{"metadata": {}}`` to intentionally clear a thread's metadata.
    """

    metadata: dict[str, JsonValue] | None = Field(
        default=None,
        description="New metadata; omit for a no-op, send ``{}`` to clear existing metadata.",
    )


class ThreadStateUpdateRequest(BaseModel):
    """Manually write/patch a thread's state (human-in-the-loop edit)."""

    values: dict[str, JsonValue] | list[JsonValue] | None = Field(
        default=None, description="State values to write into the new checkpoint."
    )
    as_node: str | None = Field(
        default=None, description="Node the update is attributed to."
    )
    checkpoint: CheckpointConfigModel | None = Field(
        default=None,
        description="Checkpoint to branch the update from; latest if omitted.",
    )


class ThreadModel(BaseModel):
    """LangGraph-compatible thread representation."""

    thread_id: UUID = Field(description="Unique identifier of the thread.")
    created_at: str = Field(
        description="ISO-8601 timestamp when the thread was created."
    )
    updated_at: str = Field(
        description="ISO-8601 timestamp when the thread row was last updated."
    )
    state_updated_at: str | None = Field(
        default=None,
        description="ISO-8601 timestamp when the thread's state last changed.",
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Metadata stored with the thread."
    )
    config: dict[str, JsonValue] = Field(
        default_factory=dict, description="Config associated with the thread."
    )
    status: ThreadStatus = Field(
        description="Thread status (idle/busy/interrupted/error)."
    )
    values: dict[str, JsonValue] | None = Field(
        default=None, description="Latest checkpoint state values, if any."
    )
    interrupts: JsonValue = Field(
        default=None, description="Pending interrupts on the thread, if any."
    )
    ttl: ThreadTtlInfo | None = Field(
        default=None, description="Resolved TTL information, if a TTL is set."
    )
    extracted: dict[str, JsonValue] | None = Field(
        default=None,
        description="Fields extracted from state per the search request's ``extract``.",
    )


class ThreadSearchRequest(BaseModel):
    """Payload for listing or searching threads."""

    ids: list[UUID] | None = Field(
        default=None, description="Restrict results to these thread ids."
    )
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="Filter by exact metadata key/value matches."
    )
    values: dict[str, JsonValue] | None = Field(
        default=None, description="Filter by exact state-value matches."
    )
    status: ThreadStatus | None = Field(
        default=None, description="Filter by thread status."
    )
    limit: int = Field(default=10, ge=1, le=1000, description="Page size (1–1000).")
    offset: int = Field(default=0, ge=0, description="Number of results to skip.")
    sort_by: (
        Literal[
            "thread_id",
            "status",
            "created_at",
            "updated_at",
            "state_updated_at",
        ]
        | None
    ) = Field(default=None, description="Field to sort by.")
    sort_order: Literal["asc", "desc"] | None = Field(
        default=None, description="Sort direction."
    )
    select: (
        list[
            Literal[
                "thread_id",
                "created_at",
                "updated_at",
                "state_updated_at",
                "metadata",
                "config",
                "status",
                "values",
                "interrupts",
            ]
        ]
        | None
    ) = Field(default=None, description="Subset of fields to return per thread.")
    extract: dict[str, str] | None = Field(
        default=None,
        description="Map of output key to state path, extracted into each result's ``extracted``.",
    )


class InterruptModel(BaseModel):
    """Serialized interrupt entry."""

    id: str | None = Field(default=None, description="Interrupt identifier, if any.")
    value: dict[str, JsonValue] = Field(
        description="Payload surfaced by the interrupt."
    )


class ThreadTaskModel(BaseModel):
    """Serialized pending task entry."""

    id: str = Field(description="Task identifier.")
    name: str = Field(description="Name of the node the task runs.")
    error: str | None = Field(
        default=None, description="Error message if the task failed."
    )
    interrupts: list[InterruptModel] = Field(
        default_factory=list, description="Interrupts raised by the task."
    )
    checkpoint: CheckpointConfigModel | None = Field(
        default=None, description="Checkpoint associated with the task, if any."
    )
    state: dict[str, JsonValue] | None = Field(
        default=None, description="Task-local state, if any."
    )


class ThreadStateModel(BaseModel):
    """Latest checkpointed state for a thread."""

    values: dict[str, JsonValue] | list[JsonValue] = Field(
        description="Checkpointed state values."
    )
    next: list[str] = Field(
        description="Nodes scheduled to run next; empty when the graph is idle."
    )
    tasks: list[ThreadTaskModel] = Field(
        default_factory=list, description="Pending tasks at this checkpoint."
    )
    checkpoint: CheckpointConfigModel = Field(
        description="Selector identifying this checkpoint."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Metadata stored with the checkpoint."
    )
    created_at: str | None = Field(
        default=None, description="ISO-8601 timestamp when the checkpoint was created."
    )
    parent_checkpoint: dict[str, JsonValue] | None = Field(
        default=None, description="Selector of the parent checkpoint, if any."
    )
    interrupts: list[InterruptModel] = Field(
        default_factory=list, description="Interrupts active at this checkpoint."
    )


class ThreadStateSearchRequest(BaseModel):
    """Payload for retrieving thread history."""

    limit: int = Field(
        default=1, ge=1, le=1000, description="Maximum checkpoints to return (1–1000)."
    )
    before: CheckpointConfigModel | None = Field(
        default=None, description="Return checkpoints recorded before this one."
    )
    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="Filter checkpoints by metadata key/value matches."
    )
    checkpoint: CheckpointConfigModel | None = Field(
        default=None, description="Restrict history to this checkpoint's namespace."
    )
