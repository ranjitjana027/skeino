"""Schemas for assistants and graph schema introspection."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from skeino.schemas.common import JsonValue


class AssistantSearchRequest(BaseModel):
    """Payload for listing assistants."""

    metadata: dict[str, JsonValue] | None = Field(
        default=None, description="Filter by exact metadata key/value matches."
    )
    graph_id: str | None = Field(default=None, description="Filter by graph id.")
    name: str | None = Field(default=None, description="Filter by assistant name.")
    limit: int = Field(default=10, ge=1, le=1000, description="Page size (1–1000).")
    offset: int = Field(default=0, ge=0, description="Number of results to skip.")
    sort_by: (
        Literal[
            "assistant_id",
            "created_at",
            "updated_at",
            "name",
            "graph_id",
        ]
        | None
    ) = Field(default=None, description="Field to sort by.")
    sort_order: Literal["asc", "desc"] | None = Field(
        default=None, description="Sort direction."
    )
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
    ) = Field(default=None, description="Subset of fields to return per assistant.")


class AssistantModel(BaseModel):
    """LangGraph-compatible assistant representation."""

    assistant_id: UUID = Field(description="Unique identifier of the assistant.")
    graph_id: str = Field(description="Graph the assistant is bound to.")
    config: dict[str, JsonValue] = Field(
        default_factory=dict, description="Config baked into the assistant."
    )
    context: dict[str, JsonValue] = Field(
        default_factory=dict, description="Context baked into the assistant."
    )
    created_at: str = Field(
        description="ISO-8601 timestamp when the assistant was created."
    )
    updated_at: str = Field(
        description="ISO-8601 timestamp when the assistant was last updated."
    )
    metadata: dict[str, JsonValue] = Field(
        default_factory=dict, description="Metadata stored with the assistant."
    )
    version: int = Field(default=1, description="Assistant version number.")
    name: str | None = Field(default=None, description="Human-readable assistant name.")
    description: str | None = Field(
        default=None, description="Human-readable assistant description."
    )


class GraphSchemaModel(BaseModel):
    """Schema description returned for an assistant."""

    graph_id: str = Field(description="Graph the schemas describe.")
    input_schema: dict[str, JsonValue] | None = Field(
        default=None, description="JSON schema for graph input, if available."
    )
    output_schema: dict[str, JsonValue] | None = Field(
        default=None, description="JSON schema for graph output, if available."
    )
    state_schema: dict[str, JsonValue] = Field(
        description="JSON schema for the graph's state."
    )
    config_schema: dict[str, JsonValue] | None = Field(
        default=None, description="JSON schema for graph config, if available."
    )
    context_schema: dict[str, JsonValue] | None = Field(
        default=None, description="JSON schema for graph context, if available."
    )
