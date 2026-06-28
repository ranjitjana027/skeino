"""Assistant search, lookup, and graph-schema routes."""

from typing import Any

from fastapi import APIRouter, Query, Request

from skeino.api._openapi import request_model
from skeino.api._request import get_state, parse_request_model
from skeino.schemas import (
    AssistantModel,
    AssistantSearchRequest,
    GraphSchemaModel,
)

router = APIRouter(prefix="/assistants")


@router.post("/search", response_model=list[AssistantModel])
@request_model(AssistantSearchRequest)
async def search_assistants(request: Request) -> list[AssistantModel]:
    """Search or list assistants."""
    payload = await parse_request_model(request, AssistantSearchRequest)
    state = get_state(request)
    return state.assistant_ops.search(payload)


@router.get("/{assistant_id}", response_model=AssistantModel)
async def get_assistant(request: Request, assistant_id: str) -> AssistantModel:
    """Return an assistant by ID."""
    state = get_state(request)
    return state.assistant_ops.get(assistant_id)


@router.get("/{assistant_id}/schemas", response_model=GraphSchemaModel)
async def get_assistant_schemas(
    request: Request, assistant_id: str
) -> GraphSchemaModel:
    """Return graph schemas for an assistant."""
    state = get_state(request)
    return state.assistant_ops.get_schemas(assistant_id)


@router.get("/{assistant_id}/graph")
async def get_assistant_graph(
    request: Request,
    assistant_id: str,
    xray: bool = Query(
        default=False, description="Expand subgraph internals in the graph."
    ),
) -> dict[str, Any]:
    """Return the graph structure (nodes, edges) for visualization."""
    state = get_state(request)
    return state.assistant_ops.get_graph(assistant_id, xray=xray)


@router.get("/{assistant_id}/subgraphs")
async def get_assistant_subgraphs(
    request: Request,
    assistant_id: str,
    recurse: bool = Query(
        default=False, description="Descend recursively into nested subgraphs."
    ),
) -> dict[str, Any]:
    """Return subgraph schemas for an assistant."""
    state = get_state(request)
    return state.assistant_ops.get_subgraphs(assistant_id, recurse=recurse)
