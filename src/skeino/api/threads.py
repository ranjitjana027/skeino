"""Thread CRUD, search, state, and history routes."""

from uuid import UUID

from fastapi import APIRouter, Query, Request, status

from skeino.api._request import get_state, parse_request_model
from skeino.schemas import (
    CheckpointConfigModel,
    ThreadCreateRequest,
    ThreadModel,
    ThreadPatchRequest,
    ThreadSearchRequest,
    ThreadStateModel,
    ThreadStateSearchRequest,
    ThreadStateUpdateRequest,
)

router = APIRouter(prefix="/threads")


@router.post("", response_model=ThreadModel)
async def create_thread(request: Request) -> ThreadModel:
    """Create a new persistent thread."""
    payload = await parse_request_model(request, ThreadCreateRequest)
    state = get_state(request)
    return await state.thread_ops.create(payload)


@router.post("/search", response_model=list[ThreadModel])
async def search_threads(request: Request) -> list[ThreadModel]:
    """Search or list threads."""
    payload = await parse_request_model(request, ThreadSearchRequest)
    state = get_state(request)
    return await state.thread_ops.search(payload)


@router.get("/{thread_id}", response_model=ThreadModel)
async def get_thread(request: Request, thread_id: UUID) -> ThreadModel:
    """Return metadata and latest values for a thread."""
    state = get_state(request)
    return await state.thread_ops.get(str(thread_id))


@router.patch("/{thread_id}", response_model=ThreadModel)
async def patch_thread(request: Request, thread_id: UUID) -> ThreadModel:
    """Update a thread's metadata."""
    payload = await parse_request_model(request, ThreadPatchRequest)
    state = get_state(request)
    return await state.thread_ops.update(str(thread_id), payload)


@router.delete("/{thread_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_thread(request: Request, thread_id: UUID) -> None:
    """Delete a thread, its runs, and its checkpoint history."""
    state = get_state(request)
    await state.thread_ops.delete(str(thread_id))


@router.post("/{thread_id}/copy", response_model=ThreadModel)
async def copy_thread(request: Request, thread_id: UUID) -> ThreadModel:
    """Fork a thread into an independent copy seeded with its latest state."""
    state = get_state(request)
    return await state.thread_ops.copy(str(thread_id))


@router.get("/{thread_id}/state", response_model=ThreadStateModel)
async def get_thread_state(
    request: Request,
    thread_id: UUID,
    subgraphs: bool = False,
) -> ThreadStateModel:
    """Return the latest checkpoint state for a thread."""
    state = get_state(request)
    return await state.thread_ops.get_state(str(thread_id), subgraphs=subgraphs)


@router.post("/{thread_id}/state", response_model=CheckpointConfigModel)
async def update_thread_state(
    request: Request, thread_id: UUID
) -> CheckpointConfigModel:
    """Write/patch a thread's state (human-in-the-loop edit)."""
    payload = await parse_request_model(request, ThreadStateUpdateRequest)
    state = get_state(request)
    return await state.thread_ops.update_state(str(thread_id), payload)


@router.post("/{thread_id}/state/checkpoint", response_model=ThreadStateModel)
async def get_thread_state_at_checkpoint(
    request: Request, thread_id: UUID
) -> ThreadStateModel:
    """Return state at a specific checkpoint, selected by a full config body."""
    payload = await parse_request_model(request, CheckpointConfigModel)
    state = get_state(request)
    return await state.thread_ops.get_state(str(thread_id), checkpoint=payload)


@router.get("/{thread_id}/state/{checkpoint_id}", response_model=ThreadStateModel)
async def get_thread_state_by_checkpoint_id(
    request: Request, thread_id: UUID, checkpoint_id: str
) -> ThreadStateModel:
    """Return state at a specific checkpoint id (time travel)."""
    state = get_state(request)
    checkpoint = CheckpointConfigModel(
        thread_id=str(thread_id), checkpoint_id=checkpoint_id
    )
    return await state.thread_ops.get_state(str(thread_id), checkpoint=checkpoint)


@router.get("/{thread_id}/history", response_model=list[ThreadStateModel])
async def get_thread_history(
    request: Request,
    thread_id: UUID,
    limit: int = Query(default=10, ge=1, le=1000),
    before: str | None = Query(default=None),
) -> list[ThreadStateModel]:
    """Return checkpoint history for a thread."""
    state = get_state(request)
    before_config: CheckpointConfigModel | None = None
    if before is not None:
        before_config = CheckpointConfigModel(
            thread_id=str(thread_id),
            checkpoint_id=before,
        )
    return await state.thread_ops.get_history(
        str(thread_id),
        limit=limit,
        before=before_config,
    )


@router.post("/{thread_id}/history", response_model=list[ThreadStateModel])
async def post_thread_history(
    request: Request, thread_id: UUID
) -> list[ThreadStateModel]:
    """Return checkpoint history for a thread using the POST variant."""
    payload = await parse_request_model(request, ThreadStateSearchRequest)
    state = get_state(request)
    return await state.thread_ops.get_history(
        str(thread_id),
        limit=payload.limit,
        before=payload.before,
        metadata=payload.metadata,
        checkpoint=payload.checkpoint,
    )
