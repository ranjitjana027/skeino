"""Server-info and generic response schemas."""

from pydantic import BaseModel


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str


class InitialMessageResponse(BaseModel):
    """Welcome message response."""

    message: str
    version: str


class ErrorResponse(BaseModel):
    """Error payload returned from the API."""

    detail: str


class ServerInfoModel(BaseModel):
    """Minimal system information exposed by the server."""

    status: str
    name: str
    version: str
