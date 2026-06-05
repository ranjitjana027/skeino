"""Helpers shared by all skeino routers."""

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar
from uuid import UUID

from fastapi import HTTPException, Request, status
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from skeino.config import SkeinoSettings
    from skeino.ops import AssistantOps, RunOps, ThreadOps

ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass
class SkeinoState:
    """Bundle of ops + settings attached to ``app.state.skeino`` on startup."""

    thread_ops: "ThreadOps"
    run_ops: "RunOps"
    assistant_ops: "AssistantOps"
    settings: "SkeinoSettings"


def get_state(request: Request) -> SkeinoState:
    """Return the skeino state bundle from ``app.state.skeino``."""
    return request.app.state.skeino  # type: ignore[return-value]


def run_location(thread_id: UUID, run_id: UUID) -> str:
    """Return the canonical URL for a run resource."""
    return f"/threads/{thread_id}/runs/{run_id}"


async def parse_request_model(request: Request, model_cls: type[ModelT]) -> ModelT:
    """Parse a JSON request body even when the client sends text/plain."""
    body = await request.body()
    if not body:
        payload: object = {}
    else:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid JSON body: {e.msg}",
            ) from e

    try:
        return model_cls.model_validate(payload)
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=json.loads(e.json()),
        ) from e
