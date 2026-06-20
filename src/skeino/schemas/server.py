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


class ServerInfoModel(BaseModel):
    """Minimal system information exposed by the server."""

    status: str = Field(description="Server status indicator.")
    name: str = Field(description="Server identity reported to SDK clients.")
    version: str = Field(description="Running skeino version.")
