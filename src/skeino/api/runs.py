"""Run create (background), wait, join, join-stream, cancel, delete, list, get, streaming.

Two routers. ``router`` carries the thread-scoped runs, where the caller owns
the thread and its checkpoint history. ``stateless_router`` carries the
top-level Platform endpoints for one-shot invocations, which run against a
thread created and deleted inside the request — see ``RunOps`` for why that
belongs on the server rather than in each client.
"""

import json
from typing import Any
from uuid import UUID

from fastapi import (
    APIRouter,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
    status,
)
from fastapi.responses import StreamingResponse
from pydantic import TypeAdapter, ValidationError

from skeino.api._openapi import request_model
from skeino.api._request import get_state, parse_request_model, run_location
from skeino.schemas import (
    CancelAction,
    JsonValue,
    RunCreateRequest,
    RunModel,
    RunStatus,
    StreamMode,
)
from skeino.serialization import serialize_value

router = APIRouter(prefix="/threads/{thread_id}")
stateless_router = APIRouter()
_STREAM_MODE_ADAPTER: TypeAdapter[StreamMode] = TypeAdapter(StreamMode)


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


def _parse_stream_modes(values: list[str] | None) -> list[str]:
    """Flatten ``stream_mode`` query values into a list of modes.

    Accepts what LangGraph Platform accepts — a single mode
    (``stream_mode=values``) or a JSON array (``stream_mode=["values","updates"]``)
    — plus a repeated parameter, which is how the JS SDK's ``URLSearchParams``
    may encode an array. Empty means "every mode".
    """
    modes: list[str] = []
    for value in values or []:
        if not value.startswith("["):
            parsed_modes = [value]
        else:
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Invalid stream_mode {value!r}: {exc}",
                ) from exc
            if not isinstance(parsed, list) or not all(
                isinstance(mode, str) for mode in parsed
            ):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Invalid stream_mode {value!r}: expected a list of strings.",
                )
            parsed_modes = parsed
        for mode in parsed_modes:
            try:
                modes.append(_STREAM_MODE_ADAPTER.validate_python(mode))
            except ValidationError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Invalid stream_mode {mode!r}.",
                ) from exc
    return modes


@router.get(
    "/runs/{run_id}/stream",
    response_class=StreamingResponse,
    responses={
        status.HTTP_200_OK: {
            "content": {"text/event-stream": {"schema": {"type": "string"}}}
        }
    },
)
async def join_run_stream(
    request: Request,
    thread_id: UUID,
    run_id: UUID,
    stream_mode: list[str] | None = Query(default=None),
    cancel_on_disconnect: bool = Query(default=False),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    """Join a run's SSE stream: replay after ``Last-Event-ID``, then tail live.

    Replay needs the run to have been created with ``stream_resumable: true``;
    ``Last-Event-ID: -1`` replays from the first event. Without the header the
    join tails live events only (LangGraph Platform semantics). A finished run
    with nothing to replay yields its final ``values`` and ``end``.
    """
    state = get_state(request)
    event_stream = await state.run_ops.join_run_stream(
        str(thread_id),
        str(run_id),
        stream_modes=_parse_stream_modes(stream_mode),
        last_event_id=last_event_id,
        cancel_on_disconnect=cancel_on_disconnect,
    )
    return StreamingResponse(
        event_stream,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Location": f"/threads/{thread_id}/runs/{run_id}/stream",
            "Content-Location": run_location(thread_id, run_id),
        },
    )


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
