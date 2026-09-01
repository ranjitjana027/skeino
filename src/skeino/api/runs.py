"""Run create, list, get, and streaming routes.

Two routers. ``router`` carries the thread-scoped runs, where the caller owns
the thread and its checkpoint history. ``stateless_router`` carries the
top-level Platform endpoints for one-shot invocations, which run against a
thread created and deleted inside the request — see ``RunOps`` for why that
belongs on the server rather than in each client.
"""

import json
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from fastapi.responses import StreamingResponse

from skeino.api._openapi import request_model
from skeino.api._request import get_state, parse_request_model, run_location
from skeino.schemas import RunCreateRequest, RunModel, RunStatus
from skeino.serialization import serialize_value

router = APIRouter(prefix="/threads/{thread_id}")
stateless_router = APIRouter()


@router.post("/runs", response_model=RunModel)
@request_model(RunCreateRequest)
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


# --- Stateless runs -------------------------------------------------------
#
# No thread id in the path and none in the response: a caller that wants a
# one-shot answer should not have to learn thread lifecycle to get it.


@stateless_router.post("/runs", response_model=RunModel)
@request_model(RunCreateRequest)
async def create_stateless_run(request: Request, response: Response) -> RunModel:
    """Execute a run on an ephemeral thread and return its metadata."""
    payload = await parse_request_model(request, RunCreateRequest)
    state = get_state(request)
    run = await state.run_ops.create_stateless_run(payload)
    if isinstance(run.metadata, dict) and "total_tokens" in run.metadata:
        response.headers["X-Tokens-Used"] = str(run.metadata["total_tokens"])
    # No Location header: the run's thread is deleted by the time this returns,
    # so /threads/{id}/runs/{run_id} would 404. Advertising a dead URL is worse
    # than advertising none.
    return run


@stateless_router.post("/runs/wait")
@request_model(RunCreateRequest)
async def wait_stateless_run(request: Request) -> Any:
    """Execute a run on an ephemeral thread and return the graph's final state."""
    payload = await parse_request_model(request, RunCreateRequest)
    state = get_state(request)
    return await state.run_ops.wait_stateless_run(payload)


@stateless_router.post("/runs/stream")
@request_model(RunCreateRequest)
async def stream_stateless_run(request: Request) -> StreamingResponse:
    """Execute a run on an ephemeral thread and stream its output as SSE."""
    payload = await parse_request_model(request, RunCreateRequest)
    state = get_state(request)
    _, event_stream = await state.run_ops.create_stateless_streaming_run(payload)
    return StreamingResponse(
        event_stream,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@stateless_router.post("/runs/batch")
async def run_stateless_batch(request: Request) -> Any:
    """Execute several stateless runs and return their final states, in order.

    The body is a JSON array of run payloads. Parsed by hand rather than
    declared, for the same reason every other run route is: the LangGraph SDK
    sends JSON under a ``text/plain`` content-type to dodge CORS preflight, and
    FastAPI's body binding rejects that.
    """
    body = await request.body()
    try:
        parsed = json.loads(body) if body else []
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid JSON body: {exc}",
        ) from exc
    if not isinstance(parsed, list):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Batch body must be a JSON array of run payloads.",
        )

    payloads = []
    for index, item in enumerate(parsed):
        try:
            payloads.append(RunCreateRequest.model_validate(item))
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Run {index} in the batch is invalid: {exc}",
            ) from exc

    state = get_state(request)
    return serialize_value(await state.run_ops.run_stateless_batch(payloads))
