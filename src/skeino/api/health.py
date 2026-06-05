"""Health, info, and initial-message routes."""

from fastapi import APIRouter, Request

from skeino.api._request import get_state
from skeino.schemas import HealthResponse, InitialMessageResponse, ServerInfoModel

router = APIRouter()


@router.get("/api/health", response_model=HealthResponse)
async def health_check(request: Request) -> HealthResponse:
    """Return application liveness status."""
    state = get_state(request)
    return HealthResponse(status="healthy", version=state.settings.server_version)


@router.get("/info", response_model=ServerInfoModel)
async def get_server_info(request: Request) -> ServerInfoModel:
    """Return server information used by LangGraph SDK clients."""
    state = get_state(request)
    return state.assistant_ops.get_server_info(state.settings.server_version)


@router.get("/api/initial-message", response_model=InitialMessageResponse)
async def get_initial_message(request: Request) -> InitialMessageResponse:
    """Return the welcome message displayed when the chat UI loads."""
    state = get_state(request)
    return InitialMessageResponse(
        message=state.settings.welcome_message or "",
        version=state.settings.server_version,
    )
