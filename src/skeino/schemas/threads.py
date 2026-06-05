"""Schemas for thread creation, search, state, and history endpoints."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from skeino.schemas.common import (
    CheckpointConfigModel,
    JsonValue,
    ThreadStatus,
)
from skeino.schemas.runs import CommandModel


class ThreadTtlConfig(BaseModel):
    """Time-to-live settings for a thread."""

    strategy: Literal["delete", "keep_latest"] = "delete"
    ttl: float | None = None


class ThreadTtlInfo(BaseModel):
    """TTL information returned for a thread."""

    strategy: Literal["delete", "keep_latest"]
    ttl_minutes: float
    expires_at: str


class ThreadSuperstepUpdate(BaseModel):
    """Initial state update applied during thread creation."""

    values: dict[str, JsonValue] | list[JsonValue] | None = None
    command: CommandModel | None = None
    as_node: str


class ThreadSuperstep(BaseModel):
    """A superstep container for thread bootstrap updates."""

    updates: list[ThreadSuperstepUpdate]


class ThreadCreateRequest(BaseModel):
    """Payload for creating a thread."""

    thread_id: UUID | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    if_exists: Literal["raise", "do_nothing"] = "raise"
    ttl: ThreadTtlConfig | None = None
    supersteps: list[ThreadSuperstep] = Field(default_factory=list)


class ThreadModel(BaseModel):
    """LangGraph-compatible thread representation."""

    thread_id: UUID
    created_at: str
    updated_at: str
    state_updated_at: str | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    config: dict[str, JsonValue] = Field(default_factory=dict)
    status: ThreadStatus
    values: dict[str, JsonValue] | None = None
    interrupts: JsonValue = None
    ttl: ThreadTtlInfo | None = None
    extracted: dict[str, JsonValue] | None = None


class ThreadSearchRequest(BaseModel):
    """Payload for listing or searching threads."""

    ids: list[UUID] | None = None
    metadata: dict[str, JsonValue] | None = None
    values: dict[str, JsonValue] | None = None
    status: ThreadStatus | None = None
    limit: int = Field(default=10, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    sort_by: (
        Literal[
            "thread_id",
            "status",
            "created_at",
            "updated_at",
            "state_updated_at",
        ]
        | None
    ) = None
    sort_order: Literal["asc", "desc"] | None = None
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
    ) = None
    extract: dict[str, str] | None = None


class InterruptModel(BaseModel):
    """Serialized interrupt entry."""

    id: str | None = None
    value: dict[str, JsonValue]


class ThreadTaskModel(BaseModel):
    """Serialized pending task entry."""

    id: str
    name: str
    error: str | None = None
    interrupts: list[InterruptModel] = Field(default_factory=list)
    checkpoint: CheckpointConfigModel | None = None
    state: dict[str, JsonValue] | None = None


class ThreadStateModel(BaseModel):
    """Latest checkpointed state for a thread."""

    values: dict[str, JsonValue] | list[JsonValue]
    next: list[str]
    tasks: list[ThreadTaskModel] = Field(default_factory=list)
    checkpoint: CheckpointConfigModel
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    created_at: str | None = None
    parent_checkpoint: dict[str, JsonValue] | None = None
    interrupts: list[InterruptModel] = Field(default_factory=list)


class ThreadStateSearchRequest(BaseModel):
    """Payload for retrieving thread history."""

    limit: int = Field(default=1, ge=1, le=1000)
    before: CheckpointConfigModel | None = None
    metadata: dict[str, JsonValue] | None = None
    checkpoint: CheckpointConfigModel | None = None
