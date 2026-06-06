"""Run create, list, get, and streaming routes."""

from uuid import UUID

from fastapi import APIRouter, Query, Request, Response
from fastapi.responses import StreamingResponse

from skeino.api._request import get_state, parse_request_model, run_location
from skeino.schemas import RunCreateRequest, RunModel, RunStatus

router = APIRouter(prefix="/threads/{thread_id}")


@router.post("/runs", response_model=RunModel)
async def create_run(
    request: Request,
    response: Response,
    thread_id: UUID,
) -> RunModel:
    """Execute a run to completion and return its metadata."""
    payload = await parse_request_model(request, RunCreateRequest)
    state = get_state(request)
    run = await state.run_ops.create_run(str(thread_id), payload)
    response.headers["Location"] = run_location(thread_id, run.run_id)
    if isinstance(run.metadata, dict) and "total_tokens" in run.metadata:
        response.headers["X-Tokens-Used"] = str(run.metadata["total_tokens"])
    return run


@router.post("/runs/stream")
async def stream_run(request: Request, thread_id: UUID) -> StreamingResponse:
    """Execute a run and stream output chunks using SSE."""
    payload = await parse_request_model(request, RunCreateRequest)
    state = get_state(request)
    run, event_stream = await state.run_ops.create_streaming_run(
        str(thread_id), payload
    )
    return StreamingResponse(
        event_stream,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Location": run_location(thread_id, run.run_id),
        },
    )


@router.get("/runs", response_model=list[RunModel])
async def list_runs(
    request: Request,
    thread_id: UUID,
    limit: int = Query(default=10, ge=1),
    offset: int = Query(default=0, ge=0),
    status: RunStatus | None = Query(default=None),
) -> list[RunModel]:
    """List persisted runs for a thread."""
    state = get_state(request)
    return await state.run_ops.list_runs(
        str(thread_id), limit=limit, offset=offset, status_value=status
    )


@router.get("/runs/{run_id}", response_model=RunModel)
async def get_run(request: Request, thread_id: UUID, run_id: UUID) -> RunModel:
    """Return a single run by ID."""
    state = get_state(request)
    return await state.run_ops.get_run(str(thread_id), str(run_id))
