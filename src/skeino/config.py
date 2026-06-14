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

    # Persistence — the SCHEME selects the backend (default "memory"); URIs are
    # connection details, never selectors. A URI without a matching scheme is
    # ignored (e.g. scheme="memory" + a postgres URI still uses in-memory).
    checkpointer_scheme: str = Field(
        default="memory",
        description="Persistence backend: 'memory' (default), 'postgres', "
        "'sqlite', 'mongodb', 'redis', or a custom registered scheme. The scheme "
        "alone decides the backend; both the checkpointer and (where a native "
        "implementation exists) the metadata store follow it.",
    )
    checkpointer_uri: str | None = Field(
        default=None,
        description="Connection string/path for the selected scheme — e.g. "
        "'postgresql://…', a SQLite path or ':memory:', or 'mongodb://…'. "
        "Ignored for the 'memory' scheme. DB backends are optional extras "
        "(skeino[postgres] / skeino[sqlite] / skeino[mongodb]).",
    )
    checkpointer_options: dict[str, object] = Field(default_factory=dict)
    allow_ephemeral_metadata: bool = Field(
        default=False,
        description="Permit a durable checkpointer to run with the in-memory "
        "metadata store (for schemes without a native metadata backend, e.g. "
        "redis or a custom checkpointer). Off by default so the split-brain "
        "(durable graph state, ephemeral thread/run list) fails loudly at startup.",
    )

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

    # Server presentation
    server_title: str = "skeino"
    server_description: str = "LangGraph-compatible HTTP API powered by skeino."
    server_version: str = "1.0.0"
    welcome_message: str | None = None

    # CORS
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])
    cors_methods: list[str] = Field(default_factory=lambda: list(DEFAULT_CORS_METHODS))
    cors_headers: list[str] = Field(default_factory=lambda: ["*"])
