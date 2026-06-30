"""Run create (background), wait, join, cancel, delete, list, get, streaming."""

from uuid import UUID

from fastapi import APIRouter, Query, Request, Response, status
from fastapi.responses import StreamingResponse

from skeino.api._openapi import request_model
from skeino.api._request import get_state, parse_request_model, run_location
from skeino.schemas import (
    CancelAction,
    JsonValue,
    RunCreateRequest,
    RunModel,
    RunStatus,
)

router = APIRouter(prefix="/threads/{thread_id}")


@router.post("/runs", response_model=RunModel)
@request_model(RunCreateRequest)
async def create_run(
    request: Request,
    response: Response,
    thread_id: UUID,
) -> RunModel:
    """Start a background run and return its (pending) metadata immediately."""
    payload = await parse_request_model(request, RunCreateRequest)
    state = get_state(request)
    run = await state.run_ops.create_run(str(thread_id), payload)
    response.headers["Location"] = run_location(thread_id, run.run_id)
    return run


@router.post("/runs/wait")
async def wait_run(
    request: Request,
    response: Response,
    thread_id: UUID,
) -> JsonValue:
    """Run to completion and return the final graph state values (output)."""
    payload = await parse_request_model(request, RunCreateRequest)
    state = get_state(request)
    output, total_tokens = await state.run_ops.wait_run(str(thread_id), payload)
    response.headers["X-Tokens-Used"] = str(total_tokens)
    return output


@router.post("/runs/stream")
@request_model(RunCreateRequest)
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


@router.get("/runs/{run_id}/join")
async def join_run(request: Request, thread_id: UUID, run_id: UUID) -> JsonValue:
    """Wait for a run to finish and return the final graph state values."""
    state = get_state(request)
    return await state.run_ops.join_run(str(thread_id), str(run_id))


@router.post("/runs/{run_id}/cancel", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_run(
    request: Request,
    thread_id: UUID,
    run_id: UUID,
    action: CancelAction = Query(default="interrupt"),
    wait: bool = Query(default=False),
) -> None:
    """Cancel an in-flight run (``interrupt`` keeps it, ``rollback`` deletes it)."""
    state = get_state(request)
    await state.run_ops.cancel_run(
        str(thread_id), str(run_id), action=action, wait=wait
    )


@router.delete("/runs/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_run(request: Request, thread_id: UUID, run_id: UUID) -> None:
    """Delete a terminal run row."""
    state = get_state(request)
    await state.run_ops.delete_run(str(thread_id), str(run_id))
