"""Server-info and generic response schemas."""

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(description="Liveness indicator, ``ok`` when the server is up.")
    version: str = Field(description="Running skeino version.")


class InitialMessageResponse(BaseModel):
    """Welcome message response."""

    message: str = Field(description="Configured welcome message for SDK clients.")
    version: str = Field(description="Running skeino version.")


class ErrorResponse(BaseModel):
    """Error payload returned from the API."""

    detail: str = Field(description="Human-readable description of the error.")


class ServerFlagsModel(BaseModel):
    """Capabilities advertised to clients, in langgraph-api's ``/info`` shape.

    LangGraph Studio reads these to decide what it can do against the server —
    tracing above all: without ``langsmith_tracing_session_on_runs`` it refuses
    to show in-Studio traces.
    """

    assistants: bool = Field(
        default=True, description="Assistant endpoints are served."
    )
    crons: bool = Field(
        default=False, description="Cron endpoints are served (skeino has none)."
    )
    langsmith: bool = Field(
        description="LangSmith tracing is configured for this server's runs."
    )
    langsmith_tracing_replicas: bool = Field(
        default=True,
        description="A run can write its trace to more than one LangSmith project.",
    )
    langsmith_tracing_session_on_runs: bool = Field(
        default=True,
        description=(
            "Runs accept ``langsmith_tracer.project_name`` and report the "
            "``langsmith_session_name`` their trace was written to."
        ),
    )


class ServerHostModel(BaseModel):
    """Where the server runs, in langgraph-api's ``/info`` shape."""

    kind: str = Field(
        default="self-hosted", description="Hosting kind; skeino is always self-hosted."
    )
    project_id: str | None = Field(default=None, description="LangSmith deployment id.")
    host_revision_id: str | None = Field(default=None, description="Host revision id.")
    revision_id: str | None = Field(default=None, description="Deployment revision id.")
    tenant_id: str | None = Field(default=None, description="LangSmith tenant id.")


class ServerInfoModel(BaseModel):
    """System information exposed by the server.

    ``status``/``name``/``version`` are skeino's own; ``langgraph_py_version``,
    ``flags`` and ``host`` follow langgraph-api so LangGraph Studio and SDK
    clients can read the server's capabilities.
    """

    status: str = Field(description="Server status indicator.")
    name: str = Field(description="Server identity reported to SDK clients.")
    version: str = Field(
        description="Server version (``SkeinoSettings.server_version``)."
    )
    langgraph_py_version: str | None = Field(
        default=None, description="Installed ``langgraph`` library version."
    )
    flags: ServerFlagsModel = Field(description="Capabilities the server supports.")
    host: ServerHostModel = Field(
        default_factory=ServerHostModel, description="Hosting details."
    )
