"""Run lifecycle: create (sync + streaming), list, get."""

import asyncio
from typing import Any, AsyncIterator, Final
from uuid import UUID, uuid4

from fastapi import HTTPException, status

from skeino.concurrency import ThreadLockManager
from skeino.ops.assistants import AssistantOps
from skeino.ops.threads import ThreadOps
from skeino.persistence import MetadataStoreProtocol
from skeino.schemas import (
    JsonValue,
    RunCreateRequest,
    RunModel,
    RunStatus,
    ThreadStatus,
)
from skeino.serialization import (
    build_thread_config,
    coerce_stream_modes,
    normalize_command_payload,
    normalize_input_payload,
    serialize_mapping,
    serialize_value,
)
from skeino.streaming import (
    STREAM_MAX_RETRIES,
    STREAM_RETRY_BACKOFF_SECS,
    Streamer,
    is_retriable_stream_error,
    sse_event,
)
from skeino.usage import total_tokens_from_messages

_THREAD_BUSY: Final[ThreadStatus] = "busy"
_THREAD_IDLE: Final[ThreadStatus] = "idle"
_THREAD_ERROR: Final[ThreadStatus] = "error"
_RUN_RUNNING: Final[RunStatus] = "running"
_RUN_SUCCESS: Final[RunStatus] = "success"
_RUN_ERROR: Final[RunStatus] = "error"
_RUN_LIST_STATUSES: Final[frozenset[str]] = frozenset(
    {"pending", "running", "error", "success", "timeout", "interrupted"}
)


class RunOps:
    """Create, stream, and inspect runs against a single graph."""

    def __init__(
        self,
        *,
        graph: Any,
        metadata_store: MetadataStoreProtocol,
        streamer: Streamer,
        thread_ops: ThreadOps,
        assistant_ops: AssistantOps,
        lock_manager: ThreadLockManager,
        logger: Any | None = None,
    ) -> None:
        """Capture every collaborator a run needs."""
        self._graph = graph
        self._metadata_store = metadata_store
        self._streamer = streamer
        self._thread_ops = thread_ops
        self._assistant_ops = assistant_ops
        self._lock_manager = lock_manager
        self._logger = logger

    async def create_run(self, thread_id: str, request: RunCreateRequest) -> RunModel:
        """Execute a run to completion and return its metadata."""
        await self._thread_ops.ensure_thread_for_run(thread_id, request.if_not_exists)
        self._assistant_ops.ensure_supported(request.assistant_id)
        self._validate_run_request(request)
        lock = self._lock_manager.get(thread_id)
        await self._lock_manager.acquire(lock, request.multitask_strategy, thread_id)
        run_row = await self._metadata_store.create_run(
            run_id=str(uuid4()),
            thread_id=thread_id,
            assistant_id=request.assistant_id,
            metadata=request.metadata,
            kwargs=self._build_run_kwargs(request),
            multitask_strategy=request.multitask_strategy,
        )
        run_id = str(run_row["run_id"])

        try:
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_BUSY
            )
            await self._metadata_store.update_run_status(run_id, _RUN_RUNNING)
            await self._execute_graph_run(thread_id, request, run_id=run_id)
            await self._metadata_store.update_run_status(run_id, _RUN_SUCCESS)
            await self._metadata_store.update_thread(
                thread_id,
                status_value=_THREAD_IDLE,
                mark_state_updated=True,
            )
        except HTTPException as exc:
            self._log_error(
                "Run %s failed for thread %s: %s", run_id, thread_id, exc.detail
            )
            await self._metadata_store.update_run_status(
                run_id, _RUN_ERROR, error=str(exc.detail)
            )
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_ERROR
            )
            raise
        except Exception as exc:
            self._log_error("Run %s failed for thread %s: %s", run_id, thread_id, exc)
            await self._metadata_store.update_run_status(
                run_id, _RUN_ERROR, error=str(exc)
            )
            await self._metadata_store.update_thread(
                thread_id, status_value=_THREAD_ERROR
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc
        finally:
            lock.release()

        run = await self.get_run(thread_id, run_id)
        # Surface token usage so the gateway can record it against the user's
        # rate-limit quota. The streaming path reports this via the 'end' event;
        # here we attach it to the run metadata for the X-Tokens-Used header.
        if isinstance(run.metadata, dict):
            run.metadata["total_tokens"] = await self._total_run_tokens(thread_id)
        return run

    async def create_streaming_run(
        self, thread_id: str, request: RunCreateRequest
    ) -> tuple[RunModel, AsyncIterator[str]]:
        """Create a run and stream its output as SSE."""
        await self._thread_ops.ensure_thread_for_run(thread_id, request.if_not_exists)
        self._assistant_ops.ensure_supported(request.assistant_id)
        self._validate_run_request(request)
        lock = self._lock_manager.get(thread_id)
        stream_modes = coerce_stream_modes(request.stream_mode)
        if "events" in stream_modes and len(stream_modes) > 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="'events' stream_mode cannot be combined with other modes.",
            )
        if (
            request.multitask_strategy in {"reject", "rollback", "interrupt"}
            and lock.locked()
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Thread {thread_id} already has an active run; "
                    f"multitask strategy {request.multitask_strategy!r} rejected."
                ),
            )

        run_row = await self._metadata_store.create_run(
            run_id=str(uuid4()),
            thread_id=thread_id,
            assistant_id=request.assistant_id,
            metadata=request.metadata,
            kwargs=self._build_run_kwargs(request),
            multitask_strategy=request.multitask_strategy,
        )
        run = self._run_row_to_model(run_row)

        async def event_stream() -> AsyncIterator[str]:
            event_id = 1
            await lock.acquire()
            try:
                await self._metadata_store.update_thread(
                    thread_id, status_value=_THREAD_BUSY
                )
                await self._metadata_store.update_run_status(
                    str(run.run_id), _RUN_RUNNING
                )
                yield sse_event(
                    "metadata",
                    {
                        "run_id": str(run.run_id),
                        "thread_id": str(thread_id),
                        "run": serialize_value(run.model_dump(mode="json")),
                    },
                    event_id,
                )
                event_id += 1

                runnable_input = self._resolve_run_input(request)
                config = build_thread_config(
                    thread_id,
                    request.config,
                    request.checkpoint,
                    run_id=str(run.run_id),
                )
                for attempt in range(STREAM_MAX_RETRIES):
                    try:
                        async for event_name, payload in self._streamer.stream(
                            runnable_input, config, request, stream_modes
                        ):
                            yield sse_event(event_name, payload, event_id)
                            event_id += 1
                        break
                    except BaseException as exc:
                        if (
                            attempt < STREAM_MAX_RETRIES - 1
                            and is_retriable_stream_error(exc)
                        ):
                            backoff = STREAM_RETRY_BACKOFF_SECS * (2**attempt)
                            self._log_warning(
                                "Stream attempt %s failed (retrying in %.1fs): %s",
                                attempt + 1,
                                backoff,
                                exc,
                            )
                            await asyncio.sleep(backoff)
                        else:
                            raise

                await self._metadata_store.update_run_status(
                    str(run.run_id), _RUN_SUCCESS
                )
                await self._metadata_store.update_thread(
                    thread_id,
                    status_value=_THREAD_IDLE,
                    mark_state_updated=True,
                )
                total_tokens = await self._total_run_tokens(thread_id)
                yield sse_event(
                    "end",
                    {
                        "run_id": str(run.run_id),
                        "status": _RUN_SUCCESS,
                        "usage": {"total_tokens": total_tokens},
                    },
                    event_id,
                )
            except asyncio.CancelledError:
                self._log_warning(
                    "Streaming run %s cancelled for thread %s", run.run_id, thread_id
                )
                await self._metadata_store.update_run_status(
                    str(run.run_id), "interrupted", error="Client disconnected."
                )
                await self._metadata_store.update_thread(
                    thread_id, status_value=_THREAD_IDLE
                )
                raise
            except Exception as exc:
                self._log_error(
                    "Streaming run %s failed for thread %s: %s",
                    run.run_id,
                    thread_id,
                    exc,
                )
                await self._metadata_store.update_run_status(
                    str(run.run_id), _RUN_ERROR, error=str(exc)
                )
                await self._metadata_store.update_thread(
                    thread_id, status_value=_THREAD_ERROR
                )
                yield sse_event(
                    "error",
                    {"detail": str(exc), "run_id": str(run.run_id)},
                    event_id,
                )
            finally:
                if lock.locked():
                    lock.release()

        return run, event_stream()

    async def list_runs(
        self,
        thread_id: str,
        *,
        limit: int,
        offset: int,
        status_value: str | None,
    ) -> list[RunModel]:
        """List run metadata rows for a thread."""
        await self._thread_ops.ensure_exists(thread_id)
        if status_value is not None and status_value not in _RUN_LIST_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Unsupported run status filter: {status_value}",
            )
        rows = await self._metadata_store.list_run_rows(
            thread_id,
            limit=limit,
            offset=offset,
            status_value=status_value,
        )
        return [self._run_row_to_model(row) for row in rows]

    async def get_run(self, thread_id: str, run_id: str) -> RunModel:
        """Return a single run metadata record."""
        await self._thread_ops.ensure_exists(thread_id)
        row = await self._metadata_store.fetch_run_row(thread_id, run_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Run {run_id} not found for thread {thread_id}.",
            )
        return self._run_row_to_model(row)

    async def _execute_graph_run(
        self, thread_id: str, request: RunCreateRequest, run_id: str | None = None
    ) -> None:
        """Execute a graph run without streaming."""
        runnable_input = self._resolve_run_input(request)
        config = build_thread_config(
            thread_id, request.config, request.checkpoint, run_id=run_id
        )
        await self._graph.ainvoke(
            runnable_input,
            config,
            context=normalize_input_payload(request.context),
            stream_mode="values",
            interrupt_before=request.interrupt_before,
            interrupt_after=request.interrupt_after,
            durability=request.durability,
        )

    async def _total_run_tokens(self, thread_id: str) -> int:
        """Compute total tokens consumed by a completed run from final state.

        Reads the latest checkpoint's raw messages (which carry provider
        ``usage_metadata`` / ``response_metadata``) and sums their token counts.
        The streaming serializer strips this data from the wire, so we recompute
        it here to surface usage explicitly. Degrades to 0 when no checkpointer
        or state is available.
        """
        try:
            config = {"configurable": {"thread_id": thread_id}}
            snapshot = await self._graph.aget_state(config)
        except Exception as exc:
            self._log_warning("Failed to read state for token usage: %s", exc)
            return 0
        values = getattr(snapshot, "values", None)
        if not isinstance(values, dict):
            return 0
        messages = values.get("messages") or []
        if not isinstance(messages, list):
            return 0
        return total_tokens_from_messages(messages)

    def _run_row_to_model(self, row: dict[str, Any]) -> RunModel:
        """Convert a run metadata row into the API response model."""
        return RunModel(
            run_id=UUID(str(row["run_id"])),
            thread_id=UUID(str(row["thread_id"])),
            assistant_id=str(row["assistant_id"]),
            created_at=row["created_at"].isoformat(),
            updated_at=row["updated_at"].isoformat(),
            status=row["status"],
            metadata=serialize_mapping(row["metadata"]),
            kwargs=serialize_mapping(row["kwargs"]),
            multitask_strategy=row["multitask_strategy"],
        )

    def _resolve_run_input(self, request: RunCreateRequest) -> Any:
        """Resolve the input or command object passed to the graph."""
        self._validate_run_request(request)
        command = normalize_command_payload(request.command)
        if command is not None:
            return command
        return normalize_input_payload(request.input)

    def _validate_run_request(self, request: RunCreateRequest) -> None:
        """Reject platform-only request options that this server does not support."""
        if request.after_seconds is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Scheduled runs are not supported by the OSS server.",
            )
        if request.webhook is not None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Webhook callbacks are not supported by the OSS server.",
            )

    def _build_run_kwargs(self, request: RunCreateRequest) -> dict[str, JsonValue]:
        """Persist the key run settings used to invoke the graph."""
        checkpoint: dict[str, JsonValue] | None = None
        if request.checkpoint is not None:
            checkpoint = serialize_mapping(request.checkpoint.model_dump(mode="python"))
        return {
            "assistant_id": request.assistant_id,
            "config": serialize_value(request.config),
            "context": serialize_value(request.context),
            "checkpoint": checkpoint,
            "stream_mode": serialize_value(request.stream_mode),
            "stream_subgraphs": request.stream_subgraphs,
            "stream_resumable": request.stream_resumable,
            "interrupt_before": serialize_value(request.interrupt_before),
            "interrupt_after": serialize_value(request.interrupt_after),
            "on_disconnect": request.on_disconnect,
            "durability": request.durability,
        }

    def _log_warning(self, msg: str, *args: Any) -> None:
        if self._logger is not None:
            self._logger.warning(msg, *args)

    def _log_error(self, msg: str, *args: Any) -> None:
        if self._logger is not None:
            self._logger.error(msg, *args)
