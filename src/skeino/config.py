"""Typed configuration consumed by ``skeino.create_app``."""

from typing import Final

from pydantic import BaseModel, ConfigDict, Field

DEFAULT_CORS_METHODS: Final[list[str]] = [
    "GET",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "OPTIONS",
]


class SkeinoSettings(BaseModel):
    """Configuration record passed to :func:`skeino.create_app`.

    Settings live in your code (typed, validated, version-controlled). For
    deployments that read from environment variables, use pydantic-settings
    in *your* project and pass the resulting object into ``SkeinoSettings``.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    # Persistence
    postgres_uri: str | None = None
    checkpointer_scheme: str | None = Field(
        default=None,
        description="Override the checkpointer scheme. Derived from postgres_uri "
        "(or 'memory' when no URI is set) when omitted.",
    )
    checkpointer_options: dict[str, object] = Field(default_factory=dict)

    # Assistant identity
    default_assistant_id: str | None = Field(
        default=None,
        description="Assistant id to use when the consumer registers a single graph "
        "without specifying one. Falls back to the first key of the graphs map.",
    )
    supported_assistant_ids: frozenset[str] | None = None
    assistant_name: str | None = None
    assistant_description: str | None = None
    assistant_namespace: str = "https://skeino.local/assistants"

    # Streaming behaviour
    agent_nodes: frozenset[str] = Field(default_factory=frozenset)
    status_field: str | None = None

    # Server presentation
    server_title: str = "skeino"
    server_description: str = "LangGraph-compatible HTTP API powered by skeino."
    server_version: str = "1.0.0"
    welcome_message: str | None = None

    # CORS
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])
    cors_methods: list[str] = Field(default_factory=lambda: list(DEFAULT_CORS_METHODS))
    cors_headers: list[str] = Field(default_factory=lambda: ["*"])
