"""Schemas for assistants and graph schema introspection."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from skeino.schemas.common import JsonValue


class AssistantSearchRequest(BaseModel):
    """Payload for listing assistants."""

    metadata: dict[str, JsonValue] | None = None
    graph_id: str | None = None
    name: str | None = None
    limit: int = Field(default=10, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    sort_by: (
        Literal[
            "assistant_id",
            "created_at",
            "updated_at",
            "name",
            "graph_id",
        ]
        | None
    ) = None
    sort_order: Literal["asc", "desc"] | None = None
    select: (
        list[
            Literal[
                "assistant_id",
                "graph_id",
                "name",
                "description",
                "config",
                "context",
                "created_at",
                "updated_at",
                "metadata",
                "version",
            ]
        ]
        | None
    ) = None


class AssistantModel(BaseModel):
    """LangGraph-compatible assistant representation."""

    assistant_id: UUID
    graph_id: str
    config: dict[str, JsonValue] = Field(default_factory=dict)
    context: dict[str, JsonValue] = Field(default_factory=dict)
    created_at: str
    updated_at: str
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    version: int = 1
    name: str | None = None
    description: str | None = None


class GraphSchemaModel(BaseModel):
    """Schema description returned for an assistant."""

    graph_id: str
    input_schema: dict[str, JsonValue] | None = None
    output_schema: dict[str, JsonValue] | None = None
    state_schema: dict[str, JsonValue]
    config_schema: dict[str, JsonValue] | None = None
    context_schema: dict[str, JsonValue] | None = None
